#!/bin/bash
# =============================================================================
# Synthetic Bank Data Upload Script
# =============================================================================
# 
# This script uploads all generated data to the appropriate Snowflake stages
# based on the comprehensive data mapping plan. It handles all data types and
# ensures proper file placement for automated processing by Snowflake tasks.
#
# Features:
# - Complete data mapping for all generated files
# - Automatic stage detection and file validation
# - Progress tracking and error handling
# - Dry run mode for testing
# - Comprehensive file counting and status reporting
#
# Usage:
#   ./upload-data.sh --CONNECTION_NAME=<my-sf-connection> [OPTIONS]
#
# Options:
#   --CONNECTION_NAME=<name>  Snowflake connection name (required)
#   --DRY_RUN                 Test run without uploading
#   --MAX_RETRIES=<n>         Number of retry attempts (default: 3)
#   --RETRY_DELAY=<s>         Seconds between retries (default: 5)
#   --TIMEOUT=<s>             Upload timeout in seconds (default: 300)
#   --BATCH_SIZE=<n>          Upload files in batches of N files (default: 100)
#   --PARALLEL_THREADS=<n>    Number of parallel uploads (default: 10, use 1 for sequential)
#   --VERBOSE                 Show detailed real-time progress
#
# Example:
#   ./upload-data.sh --CONNECTION_NAME=<my-sf-connection>
#   ./upload-data.sh --CONNECTION_NAME=<my-sf-connection> --DRY_RUN
#   ./upload-data.sh --CONNECTION_NAME=<my-sf-connection> --PARALLEL_THREADS=20
#   ./upload-data.sh --CONNECTION_NAME=<my-sf-connection> --MAX_RETRIES=5 --TIMEOUT=600
# =============================================================================

set -e

# --- Default values ---
CONNECTION_NAME=""
SOURCE_DATABASE="${SOURCE_DATABASE:-AAA_DEV_SYNTHETIC_BANK}"
DRY_RUN=false
MAX_RETRIES=3
RETRY_DELAY=5
UPLOAD_TIMEOUT=300
BATCH_SIZE=200  # Upload files in batches to improve stability
PARALLEL_THREADS=10  # Number of parallel upload jobs
VERBOSE=false  # Show detailed progress

# --- Dynamic path detection ---
# Get the directory where this script is located (works from any location)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Generated data is always in the 'generated_data' subdirectory relative to the script
# This makes the script portable and works regardless of where it's executed from
GENERATED_DATA_DIR="$SCRIPT_DIR/generated_data"

# --- Tracking variables ---
TOTAL_UPLOADS=0
SUCCESSFUL_UPLOADS=0
FAILED_UPLOADS=0
declare -a FAILED_ITEMS
declare -a UPLOAD_JOBS  # Track background job PIDs

# --- Parallel execution control ---
RESULTS_DIR=$(mktemp -d)
JOB_COUNTER=0

# --- Cleanup on exit ---
cleanup() {
    # Kill any remaining background jobs
    if [[ ${#UPLOAD_JOBS[@]} -gt 0 ]]; then
        for pid in "${UPLOAD_JOBS[@]}"; do
            kill "$pid" 2>/dev/null || true
        done
    fi
    
    # Remove temporary directory
    rm -rf "$RESULTS_DIR" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# --- Parse arguments ---
for ARG in "$@"; do
    case $ARG in
        --CONNECTION_NAME=*)
            CONNECTION_NAME="${ARG#*=}"
            ;;
        --DRY_RUN)
            DRY_RUN=true
            ;;
        --MAX_RETRIES=*)
            MAX_RETRIES="${ARG#*=}"
            ;;
        --RETRY_DELAY=*)
            RETRY_DELAY="${ARG#*=}"
            ;;
        --TIMEOUT=*)
            UPLOAD_TIMEOUT="${ARG#*=}"
            ;;
        --BATCH_SIZE=*)
            BATCH_SIZE="${ARG#*=}"
            ;;
        --PARALLEL_THREADS=*)
            PARALLEL_THREADS="${ARG#*=}"
            ;;
        --VERBOSE)
            VERBOSE=true
            ;;
        *)
            echo "[ERROR] Unknown argument: $ARG"
            echo "Usage: $0 --CONNECTION_NAME=<name> [OPTIONS]"
            echo "See script header for all available options"
            exit 1
            ;;
    esac
done

# --- Validate required inputs ---
if [[ -z "$CONNECTION_NAME" ]]; then
    echo "[ERROR] Missing required argument: --CONNECTION_NAME"
    echo "Usage: $0 --CONNECTION_NAME=<name> [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --CONNECTION_NAME=<name>  Snowflake connection name (required)"
    echo "  --DRY_RUN                 Test run without uploading"
    echo "  --MAX_RETRIES=<n>         Number of retry attempts (default: 3)"
    echo "  --RETRY_DELAY=<s>         Seconds between retries (default: 5)"
    echo "  --TIMEOUT=<s>             Upload timeout in seconds (default: 300)"
    echo "  --BATCH_SIZE=<n>          Upload files in batches of N (default: 100)"
    echo "  --PARALLEL_THREADS=<n>    Number of parallel uploads (default: 10)"
    echo "  --VERBOSE                 Show detailed real-time progress"
    exit 1
fi

# --- Validate generated data directory ---
if [[ ! -d "$GENERATED_DATA_DIR" ]]; then
    echo "[ERROR] Generated data directory not found: $GENERATED_DATA_DIR"
    echo "Please run the data generation first: python main.py --help"
    echo "Expected location: $GENERATED_DATA_DIR"
    exit 1
fi

# =============================================================================
# FUNCTION DEFINITIONS (must be before they're called)
# =============================================================================

# --- Function to check if timeout command is available ---
check_timeout_available() {
    if command -v timeout &> /dev/null; then
        return 0
    elif command -v gtimeout &> /dev/null; then
        # GNU timeout on macOS (via brew install coreutils)
        return 0
    else
        return 1
    fi
}

# --- Function to get the timeout command name ---
get_timeout_cmd() {
    if command -v timeout &> /dev/null; then
        echo "timeout"
    elif command -v gtimeout &> /dev/null; then
        echo "gtimeout"
    else
        echo ""
    fi
}

# =============================================================================
# STARTUP BANNER & CONFIGURATION
# =============================================================================

echo "================================================================"
echo "=== Synthetic Bank Data Upload"
echo "================================================================"
echo "Data Directory: $GENERATED_DATA_DIR"
echo "Connection: $CONNECTION_NAME"
echo "Dry Run: $DRY_RUN"
echo "Verbose Mode: $VERBOSE"
echo ""
echo "Configuration:"
echo "  Max Retries: $MAX_RETRIES"
echo "  Retry Delay: ${RETRY_DELAY}s"
echo "  Batch Size: $BATCH_SIZE files per batch"
echo "  Parallel Threads: $PARALLEL_THREADS"

# Check if timeout is available
if check_timeout_available; then
    timeout_cmd=$(get_timeout_cmd)
    echo "  Upload Timeout: ${UPLOAD_TIMEOUT}s (using $timeout_cmd)"
else
    echo "  Upload Timeout: Disabled (timeout command not available)"
    echo "  [INFO] To enable timeout on macOS: brew install coreutils"
fi
echo ""

# Show parallelization info
if [[ $PARALLEL_THREADS -gt 1 ]]; then
    echo "Parallel Upload Mode: ENABLED"
    echo "  Concurrent uploads: $PARALLEL_THREADS"

else
    echo "Parallel Upload Mode: DISABLED (sequential)"
fi
echo ""

# --- Function to test Snowflake connection ---
test_connection() {
    echo "Testing Snowflake connection..."
    
    local max_attempts=3
    local attempt=1
    
    while [[ $attempt -le $max_attempts ]]; do
        echo "  Attempt $attempt/$max_attempts..."
        
        set +e
        local test_result=$(snow sql -c "$CONNECTION_NAME" -q "SELECT CURRENT_VERSION();" 2>&1)
        local exit_code=$?
        set -e
        
        if [[ $exit_code -eq 0 ]]; then
            echo "  [OK] Connection successful!"
            echo ""
            return 0
        else
            echo "  [WARN] Connection failed (attempt $attempt/$max_attempts)"
            if [[ $attempt -lt $max_attempts ]]; then
                local wait_time=$((RETRY_DELAY * attempt))
                echo "  Waiting ${wait_time}s before retry..."
                sleep $wait_time
            fi
            attempt=$((attempt + 1))
        fi
    done
    
    echo "  [FAIL] Failed to establish connection after $max_attempts attempts"
    echo "  Please check your network connection and Snowflake credentials"
    return 1
}

# --- Parallel Job Control Functions ---

# Wait for available job slot
wait_for_job_slot() {
    while true; do
        local running_jobs=$(jobs -r | wc -l | tr -d ' ')
        if [[ $running_jobs -lt $PARALLEL_THREADS ]]; then
            break
        fi
        sleep 0.1
    done
}

# Show real-time progress with detailed status
show_progress() {
    local total_expected=$1
    local start_time=$(date +%s)
    local last_completed=0
    local activity_log="$RESULTS_DIR/activity.log"
    
    while true; do
        local completed=$(find "$RESULTS_DIR" -name "*.result" 2>/dev/null | wc -l | tr -d ' ')
        local running=$(jobs -r | wc -l | tr -d ' ')
        local elapsed=$(($(date +%s) - start_time))
        
        # Calculate ETA and throughput
        local eta_msg=""
        local throughput_msg=""
        if [[ $completed -gt 0 && $completed -lt $total_expected ]]; then
            local avg_time=$((elapsed / completed))
            local remaining=$((total_expected - completed))
            local eta=$((remaining * avg_time))
            eta_msg=" | ETA: ${eta}s"
            
            # Calculate uploads per minute
            if [[ $elapsed -gt 0 ]]; then
                local per_min=$(( (completed * 60) / elapsed ))
                throughput_msg=" | Rate: ${per_min}/min"
            fi
        fi
        
        # Show newly completed uploads in verbose mode
        if [[ "$VERBOSE" == "true" && $completed -gt $last_completed ]]; then
            echo ""
            # Find and display recently completed results
            for result_file in "$RESULTS_DIR"/*.result; do
                if [[ -f "$result_file" && "$result_file" -nt "$activity_log" ]]; then
                    IFS='|' read -r status description stage duration files_count < "$result_file"
                    local timestamp=$(date "+%H:%M:%S")
                    if [[ "$status" == "SUCCESS" ]]; then
                        echo "  [$timestamp] ✓ $description -> $stage ($files_count files, ${duration}s)"
                    elif [[ "$status" == "FAILED" ]]; then
                        echo "  [$timestamp] ✗ $description -> $stage (FAILED after ${duration}s)"
                    fi
                fi
            done
            touch "$activity_log"
        fi
        
        # Progress bar
        local percent=0
        if [[ $total_expected -gt 0 ]]; then
            percent=$(( (completed * 100) / total_expected ))
        fi
        local bar_width=30
        local filled=$(( (percent * bar_width) / 100 ))
        local empty=$((bar_width - filled))
        local bar=$(printf "%${filled}s" | tr ' ' '█')$(printf "%${empty}s" | tr ' ' '░')
        
        echo -ne "\r  [PROGRESS] $bar $percent% | $completed/$total_expected done | $running active$throughput_msg$eta_msg | ${elapsed}s    "
        
        if [[ $completed -ge $total_expected ]]; then
            echo ""
            break
        fi
        
        last_completed=$completed
        sleep 2
    done
}

# Wait for all background jobs
wait_all_uploads() {
    local total_jobs=${#UPLOAD_JOBS[@]}
    
    if [[ $total_jobs -eq 0 ]]; then
        return 0
    fi
    
    echo ""
    echo "================================================================"
    echo "=== PARALLEL UPLOAD EXECUTION ($total_jobs jobs)"
    echo "================================================================"
    
    if [[ "$VERBOSE" == "true" ]]; then
        echo "Real-time activity log:"
        echo "----------------------------------------------------------------"
    fi
    
    # Start progress monitor in background
    show_progress $total_jobs &
    local progress_pid=$!
    
    # In verbose mode, also show live status log
    local tail_pid=""
    if [[ "$VERBOSE" == "true" ]]; then
        touch "$RESULTS_DIR/status.log"
        tail -f "$RESULTS_DIR/status.log" 2>/dev/null &
        tail_pid=$!
    fi
    
    # Wait for all upload jobs
    for pid in "${UPLOAD_JOBS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    
    # Stop progress monitor and tail
    kill "$progress_pid" 2>/dev/null || true
    wait "$progress_pid" 2>/dev/null || true
    
    if [[ -n "$tail_pid" ]]; then
        kill "$tail_pid" 2>/dev/null || true
        wait "$tail_pid" 2>/dev/null || true
    fi
    
    echo ""
    echo "[INFO] All parallel uploads completed!"
    echo ""
}

# Collect results from parallel jobs
collect_parallel_results() {
    echo "Collecting results from parallel uploads..."
    echo ""
    
    for result_file in "$RESULTS_DIR"/*.result; do
        if [[ -f "$result_file" ]]; then
            IFS='|' read -r status description stage duration files_count < "$result_file"
            
            TOTAL_UPLOADS=$((TOTAL_UPLOADS + 1))
            
            if [[ "$status" == "SUCCESS" ]]; then
                SUCCESSFUL_UPLOADS=$((SUCCESSFUL_UPLOADS + 1))
                echo "  [OK] $description -> $stage ($files_count files, ${duration}s)"
            else
                FAILED_UPLOADS=$((FAILED_UPLOADS + 1))
                FAILED_ITEMS+=("$description -> $stage")
                echo "  [FAIL] $description -> $stage (${duration}s)"
            fi
            
            rm -f "$result_file"
        fi
    done
    
    echo ""
}

# --- Function to execute SQL with retry logic ---
execute_sql_with_retry() {
    local sql_command="$1"
    local description="$2"
    local attempt=1
    
    # Get timeout command if available
    local timeout_cmd=$(get_timeout_cmd)
    
    while [[ $attempt -le $MAX_RETRIES ]]; do
        if [[ $attempt -gt 1 ]]; then
            local wait_time=$((RETRY_DELAY * attempt))
            echo "  [RETRY] Attempt $attempt/$MAX_RETRIES (waiting ${wait_time}s)..."
            sleep $wait_time
        fi
        
        set +e
        if [[ -n "$timeout_cmd" ]]; then
            # Use timeout if available
            $timeout_cmd $UPLOAD_TIMEOUT snow sql -c "$CONNECTION_NAME" -q "$sql_command" 2>&1
            local result=$?
        else
            # Run without timeout on macOS (fallback)
            snow sql -c "$CONNECTION_NAME" -q "$sql_command" 2>&1
            local result=$?
        fi
        set -e
        
        if [[ $result -eq 0 ]]; then
            return 0
        elif [[ $result -eq 124 ]]; then
            echo "  [WARN] Upload timeout after ${UPLOAD_TIMEOUT}s"
        else
            echo "  [WARN] Upload failed with exit code $result"
        fi
        
        attempt=$((attempt + 1))
    done
    
    echo "  [FAIL] Failed after $MAX_RETRIES attempts: $description"
    return 1
}

# --- Core upload execution function (used by both sequential and parallel) ---
execute_upload() {
    local local_pattern="$1"
    local stage_name="$2"
    local schema="$3"
    local description="$4"
    local job_id="$5"
    local result_file="$6"
    
    local start_time=$(date +%s)
    local status_log="$RESULTS_DIR/status.log"
    
    # Log start
    echo "[$(date "+%H:%M:%S")] [JOB $job_id] Starting: $description" >> "$status_log"
    
    # Find files matching the pattern
    local files_found=0
    for file in $local_pattern; do
        if [[ -f "$file" ]]; then
            files_found=$((files_found + 1))
        fi
    done
    
    if [[ $files_found -eq 0 ]]; then
        echo "SKIPPED|$description|$stage_name|0|0" > "$result_file"
        echo "[$(date "+%H:%M:%S")] [JOB $job_id] Skipped: No files found" >> "$status_log"
        return 0
    fi
    
    # Log file count
    echo "[$(date "+%H:%M:%S")] [JOB $job_id] Uploading $files_found files..." >> "$status_log"
    
    # Upload files to stage with retry logic
    local sql_command="
        USE DATABASE ${SOURCE_DATABASE:-AAA_DEV_SYNTHETIC_BANK};
        USE SCHEMA $schema;
        PUT file://$local_pattern @$stage_name AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL=8;
    "
    
    local upload_result=0
    if execute_sql_with_retry "$sql_command" "$description" > /dev/null 2>&1; then
        upload_result=0
    else
        upload_result=1
    fi
    
    local duration=$(($(date +%s) - start_time))
    
    if [[ $upload_result -eq 0 ]]; then
        echo "SUCCESS|$description|$stage_name|$duration|$files_found" > "$result_file"
        echo "[$(date "+%H:%M:%S")] [JOB $job_id] ✓ Completed: $files_found files in ${duration}s" >> "$status_log"
    else
        echo "FAILED|$description|$stage_name|$duration|$files_found" > "$result_file"
        echo "[$(date "+%H:%M:%S")] [JOB $job_id] ✗ Failed after ${duration}s" >> "$status_log"
    fi
    
    return $upload_result
}

# --- Function to upload files to stage ---
upload_to_stage() {
    local local_pattern="$1"
    local stage_name="$2"
    local schema="$3"
    local description="$4"
    
    JOB_COUNTER=$((JOB_COUNTER + 1))
    local job_id=$JOB_COUNTER
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[$job_id] [DRY RUN] Would upload: $description -> $stage_name"
        TOTAL_UPLOADS=$((TOTAL_UPLOADS + 1))
        SUCCESSFUL_UPLOADS=$((SUCCESSFUL_UPLOADS + 1))
        return 0
    fi
    
    # Check if files exist before launching job
    local files_exist=false
    for file in $local_pattern; do
        if [[ -f "$file" ]]; then
            files_exist=true
            break
        fi
    done
    
    if [[ "$files_exist" == "false" ]]; then
        echo "[$job_id] [SKIP] No files found: $description"
        return 0
    fi
    
    # Parallel mode: Launch background job
    if [[ $PARALLEL_THREADS -gt 1 ]]; then
        # Wait for available slot
        wait_for_job_slot
        
        local result_file="$RESULTS_DIR/job_${job_id}.result"
        
        echo "[$job_id] [QUEUED] $description -> $stage_name"
        
        # Launch background job
        (
            execute_upload "$local_pattern" "$stage_name" "$schema" "$description" "$job_id" "$result_file"
        ) &
        
        # Store PID
        UPLOAD_JOBS+=($!)
        
    # Sequential mode: Execute immediately
    else
        echo "[$job_id] Uploading $description..."
        echo "  Pattern: $local_pattern"
        echo "  Stage: $stage_name"
        echo "  Schema: $schema"
        
        TOTAL_UPLOADS=$((TOTAL_UPLOADS + 1))
        
        local result_file=$(mktemp)
        
        if execute_upload "$local_pattern" "$stage_name" "$schema" "$description" "$job_id" "$result_file"; then
            IFS='|' read -r status desc stage dur files_count < "$result_file"
            echo "  [OK] Success: $files_count files uploaded in ${dur}s"
            SUCCESSFUL_UPLOADS=$((SUCCESSFUL_UPLOADS + 1))
        else
            IFS='|' read -r status desc stage dur files_count < "$result_file"
            echo "  [FAIL] Failed after all retries"
            FAILED_UPLOADS=$((FAILED_UPLOADS + 1))
            FAILED_ITEMS+=("$description -> $stage_name")
        fi
        
        rm -f "$result_file"
        echo ""
    fi
    
    return 0
}

# --- Core execution for single files upload (used by both sequential and parallel) ---
execute_single_files_upload() {
    local source_dir="$1"
    local stage_name="$2"
    local schema="$3"
    local description="$4"
    local file_pattern="$5"
    local job_id="$6"
    local result_file="$7"
    
    local start_time=$(date +%s)
    local status_log="$RESULTS_DIR/status.log"
    
    # Log start
    echo "[$(date "+%H:%M:%S")] [JOB $job_id] Starting: $description" >> "$status_log"
    
    # Count files
    local file_count=$(find "$source_dir" -name "$file_pattern" -type f 2>/dev/null | wc -l | tr -d ' ')
    
    if [[ $file_count -eq 0 ]]; then
        echo "SKIPPED|$description|$stage_name|0|0" > "$result_file"
        echo "[$(date "+%H:%M:%S")] [JOB $job_id] Skipped: No files found in $source_dir" >> "$status_log"
        return 0
    fi
    
    # Log file count
    echo "[$(date "+%H:%M:%S")] [JOB $job_id] Found $file_count files, uploading..." >> "$status_log"
    
    # Determine if batching is needed
    local upload_result=0
    
    if [[ $file_count -le $BATCH_SIZE ]]; then
        # Small batch - upload all at once
        local sql_command="
            USE DATABASE ${SOURCE_DATABASE:-AAA_DEV_SYNTHETIC_BANK};
            USE SCHEMA $schema;
            PUT file://$source_dir/$file_pattern @$stage_name AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL=8;
        "
        
        if execute_sql_with_retry "$sql_command" "$description" > /dev/null 2>&1; then
            upload_result=0
        else
            upload_result=1
        fi
    else
        # Large batch - split into smaller batches
        local num_batches=$(( (file_count + BATCH_SIZE - 1) / BATCH_SIZE ))
        
        # Get all files into an array
        local files=()
        while IFS= read -r -d '' file; do
            files+=("$file")
        done < <(find "$source_dir" -name "$file_pattern" -type f -print0 2>/dev/null | sort -z)
        
        local batch_num=1
        local total_uploaded=0
        
        # Process files in batches
        for ((i=0; i<${#files[@]}; i+=BATCH_SIZE)); do
            local batch_files=("${files[@]:i:BATCH_SIZE}")
            local batch_count=${#batch_files[@]}
            
            # Create temporary directory for batch
            local temp_batch_dir=$(mktemp -d)
            
            # Create symlinks to batch files
            for file in "${batch_files[@]}"; do
                ln -s "$file" "$temp_batch_dir/"
            done
            
            # Upload batch
            local batch_sql="
                USE DATABASE ${SOURCE_DATABASE:-AAA_DEV_SYNTHETIC_BANK};
                USE SCHEMA $schema;
                PUT file://$temp_batch_dir/* @$stage_name AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL=8;
            "
            
            if execute_sql_with_retry "$batch_sql" "$description (batch $batch_num/$num_batches)" > /dev/null 2>&1; then
                total_uploaded=$((total_uploaded + batch_count))
            else
                upload_result=1
            fi
            
            # Clean up temporary directory
            rm -rf "$temp_batch_dir"
            
            # Brief pause between batches
            if [[ $batch_num -lt $num_batches ]]; then
                sleep 2
            fi
            
            batch_num=$((batch_num + 1))
        done
        
        # Check if all files were uploaded
        if [[ $total_uploaded -lt $file_count ]]; then
            upload_result=1
        fi
    fi
    
    local duration=$(($(date +%s) - start_time))
    
    if [[ $upload_result -eq 0 ]]; then
        echo "SUCCESS|$description|$stage_name|$duration|$file_count" > "$result_file"
        echo "[$(date "+%H:%M:%S")] [JOB $job_id] ✓ Completed: $file_count files in ${duration}s" >> "$status_log"
    else
        echo "FAILED|$description|$stage_name|$duration|$file_count" > "$result_file"
        echo "[$(date "+%H:%M:%S")] [JOB $job_id] ✗ Failed after ${duration}s" >> "$status_log"
    fi
    
    return $upload_result
}

# --- Function to upload single files ---
upload_single_files() {
    local source_dir="$1"
    local stage_name="$2"
    local schema="$3"
    local description="$4"
    local file_pattern="$5"
    
    JOB_COUNTER=$((JOB_COUNTER + 1))
    local job_id=$JOB_COUNTER
    
    # Quick directory check
    if [[ ! -d "$source_dir" ]]; then
        echo "[$job_id] [SKIP] Directory not found: $source_dir"
        return 0
    fi
    
    # Count files
    local file_count=$(find "$source_dir" -name "$file_pattern" -type f 2>/dev/null | wc -l | tr -d ' ')
    
    if [[ $file_count -eq 0 ]]; then
        echo "[$job_id] [SKIP] No files found in $source_dir matching $file_pattern"
        return 0
    fi
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo "[$job_id] [DRY RUN] Would upload $file_count files: $description"
        TOTAL_UPLOADS=$((TOTAL_UPLOADS + 1))
        SUCCESSFUL_UPLOADS=$((SUCCESSFUL_UPLOADS + 1))
        return 0
    fi
    
    # Parallel mode: Launch background job
    if [[ $PARALLEL_THREADS -gt 1 ]]; then
        # Wait for available slot
        wait_for_job_slot
        
        local result_file="$RESULTS_DIR/job_${job_id}.result"
        
        echo "[$job_id] [QUEUED] $description ($file_count files) -> $stage_name"
        
        # Launch background job
        (
            execute_single_files_upload "$source_dir" "$stage_name" "$schema" "$description" "$file_pattern" "$job_id" "$result_file"
        ) &
        
        # Store PID
        UPLOAD_JOBS+=($!)
        
        return 0
    fi
    
    # Sequential mode: Execute immediately
    TOTAL_UPLOADS=$((TOTAL_UPLOADS + 1))
    
    echo "[$job_id] Uploading $description..."
    echo "  Directory: $source_dir"
    echo "  Stage: $stage_name"
    echo "  Schema: $schema"
    echo "  Pattern: $file_pattern"
    echo "  Found $file_count files"
    
    local result_file=$(mktemp)
    
    if execute_single_files_upload "$source_dir" "$stage_name" "$schema" "$description" "$file_pattern" "$job_id" "$result_file"; then
        IFS='|' read -r status desc stage dur files_count < "$result_file"
        if [[ "$status" == "SUCCESS" ]]; then
            echo "  [OK] Success: $files_count files uploaded in ${dur}s"
            SUCCESSFUL_UPLOADS=$((SUCCESSFUL_UPLOADS + 1))
        else
            echo "  [SKIP] $status"
        fi
    else
        IFS='|' read -r status desc stage dur files_count < "$result_file"
        echo "  [FAIL] Failed after all retries"
        FAILED_UPLOADS=$((FAILED_UPLOADS + 1))
        FAILED_ITEMS+=("$description -> $stage_name")
    fi
    
    rm -f "$result_file"
    echo ""
    return 0
}

# --- Test connection before starting uploads ---
if [[ "$DRY_RUN" != "true" ]]; then
    if ! test_connection; then
        echo "[ERROR] Cannot proceed without a valid Snowflake connection"
        exit 1
    fi
fi

# =============================================================================
# UPLOAD CUSTOMER & ACCOUNT DATA
# =============================================================================
echo "=== CUSTOMER & ACCOUNT DATA ==="
echo ""

# Customer master data
upload_to_stage \
    "$GENERATED_DATA_DIR/master_data/customers.csv" \
    "CRMI_RAW_ST_CUSTOMERS" \
    "CRM_RAW_001" \
    "Customer Master Data"

# Customer addresses (SCD Type 2)
upload_to_stage \
    "$GENERATED_DATA_DIR/master_data/customer_addresses.csv" \
    "CRMI_RAW_ST_ADDRESSES" \
    "CRM_RAW_001" \
    "Customer Addresses (SCD Type 2)"

# Customer address updates (SCD Type 2 historical changes)
upload_single_files \
    "$GENERATED_DATA_DIR/master_data/address_updates" \
    "CRMI_RAW_ST_ADDRESSES" \
    "CRM_RAW_001" \
    "Customer Address Updates (SCD Type 2)" \
    "customer_addresses_*.csv"

# Customer attribute updates (SCD Type 2 - employment, account tier, etc.)
upload_single_files \
    "$GENERATED_DATA_DIR/master_data/customer_updates" \
    "CRMI_RAW_ST_CUSTOMERS" \
    "CRM_RAW_001" \
    "Customer Attribute Updates (SCD Type 2)" \
    "customer_updates_*.csv"

# Exposed Person compliance data
upload_to_stage \
    "$GENERATED_DATA_DIR/master_data/pep_data.csv" \
    "CRMI_RAW_ST_EXPOSED_PERSON" \
    "CRM_RAW_001" \
    "Exposed Person Compliance Data"

# Customer lifecycle events (date-based files)
upload_single_files \
    "$GENERATED_DATA_DIR/master_data/customer_events" \
    "CRMI_RAW_ST_CUSTOMER_EVENTS" \
    "CRM_RAW_001" \
    "Customer Lifecycle Events (by date)" \
    "customer_events_*.csv"

# Customer status history
upload_to_stage \
    "$GENERATED_DATA_DIR/master_data/customer_status.csv" \
    "CRMI_RAW_ST_CUSTOMER_EVENTS" \
    "CRM_RAW_001" \
    "Customer Status History"

# Account master data
upload_to_stage \
    "$GENERATED_DATA_DIR/master_data/accounts.csv" \
    "ACCI_RAW_ST_ACCOUNTS" \
    "CRM_RAW_001" \
    "Account Master Data"

# Employee master data
upload_to_stage \
    "$GENERATED_DATA_DIR/master_data/employees.csv" \
    "EMPI_RAW_ST_EMPLOYEES" \
    "CRM_RAW_001" \
    "Employee Master Data"

# Refresh stream metadata for employee files
if [[ "$DRY_RUN" != "true" ]]; then
    echo "  [INFO] Refreshing stream metadata for EMPI_RAW_SM_EMPLOYEE_FILES..."
    snow sql -c "$CONNECTION_NAME" -q "
        USE DATABASE ${SOURCE_DATABASE:-AAA_DEV_SYNTHETIC_BANK};
        USE SCHEMA CRM_RAW_001;
        SELECT SYSTEM\$STREAM_HAS_DATA('EMPI_RAW_SM_EMPLOYEE_FILES') AS has_data;
    " > /dev/null 2>&1
fi

# Client-advisor assignments
upload_to_stage \
    "$GENERATED_DATA_DIR/master_data/client_assignments.csv" \
    "EMPI_RAW_ST_CLIENT_ASSIGNMENTS" \
    "CRM_RAW_001" \
    "Client-Advisor Assignments"

# Refresh stream metadata for assignment files
if [[ "$DRY_RUN" != "true" ]]; then
    echo "  [INFO] Refreshing stream metadata for EMPI_RAW_SM_ASSIGNMENT_FILES..."
    snow sql -c "$CONNECTION_NAME" -q "
        USE DATABASE ${SOURCE_DATABASE:-AAA_DEV_SYNTHETIC_BANK};
        USE SCHEMA CRM_RAW_001;
        SELECT SYSTEM\$STREAM_HAS_DATA('EMPI_RAW_SM_ASSIGNMENT_FILES') AS has_data;
    " > /dev/null 2>&1
fi

# =============================================================================
# UPLOAD REFERENCE DATA
# =============================================================================
echo "=== REFERENCE DATA ==="
echo ""

# FX Rates
upload_single_files \
    "$GENERATED_DATA_DIR/fx_rates" \
    "REFI_RAW_ST_FX_RATES" \
    "REF_RAW_001" \
    "FX Rates" \
    "fx_rates_*.csv"

# =============================================================================
# UPLOAD PAYMENT DATA
# =============================================================================
echo "=== PAYMENT DATA ==="
echo ""

# Payment transactions
upload_single_files \
    "$GENERATED_DATA_DIR/payment_transactions" \
    "PAYI_RAW_ST_TRANSACTIONS" \
    "PAY_RAW_001" \
    "Payment Transactions" \
    "pay_transactions_*.csv"

# SWIFT ISO20022 messages
upload_single_files \
    "$GENERATED_DATA_DIR/swift_messages" \
    "ICGI_RAW_ST_SWIFT_INBOUND" \
    "PAY_RAW_001" \
    "SWIFT ISO20022 Messages" \
    "*.xml"

# =============================================================================
# UPLOAD TRADING DATA
# =============================================================================
echo "=== TRADING DATA ==="
echo ""

# Equity trades
upload_single_files \
    "$GENERATED_DATA_DIR/equity_trades" \
    "EQTI_RAW_ST_TRADES" \
    "EQT_RAW_001" \
    "Equity Trades" \
    "trades_*.csv"

# Fixed Income trades
upload_single_files \
    "$GENERATED_DATA_DIR/fixed_income_trades" \
    "FIII_RAW_ST_TRADES" \
    "FII_RAW_001" \
    "Fixed Income Trades" \
    "fixed_income_trades_*.csv"

# Commodity trades
upload_single_files \
    "$GENERATED_DATA_DIR/commodity_trades" \
    "CMDI_RAW_ST_TRADES" \
    "CMD_RAW_001" \
    "Commodity Trades" \
    "commodity_trades_*.csv"

# =============================================================================
# UPLOAD LOAN DOCUMENTS
# =============================================================================
echo "=== LOAN DOCUMENTS ==="
echo ""

# Email documents
upload_single_files \
    "$GENERATED_DATA_DIR/emails" \
    "LOAI_RAW_ST_EMAIL_INBOUND" \
    "LOA_RAW_001" \
    "Loan Email Documents" \
    "*.txt"

# PDF documents
upload_single_files \
    "$GENERATED_DATA_DIR/creditcard_pdf" \
    "LOAI_RAW_ST_PDF_INBOUND" \
    "LOA_RAW_001" \
    "Loan PDF Documents" \
    "*.pdf"

# =============================================================================
# UPLOAD LCR DATA (FINMA Liquidity Coverage Ratio)
# =============================================================================
echo "=== LCR DATA (FINMA LIQUIDITY COVERAGE RATIO) ==="
echo ""

# HQLA Holdings (High-Quality Liquid Assets)
upload_single_files \
    "$GENERATED_DATA_DIR/lcr" \
    "LIQI_RAW_ST_HQLA_HOLDINGS" \
    "REP_RAW_001" \
    "LCR HQLA Holdings" \
    "hqla_holdings_*.csv"

# Deposit Balances (for net cash outflow calculation)
upload_single_files \
    "$GENERATED_DATA_DIR/lcr" \
    "LIQI_RAW_ST_DEPOSIT_BALANCES" \
    "REP_RAW_001" \
    "LCR Deposit Balances" \
    "deposit_balances_*.csv"

# Refresh stream metadata for LCR files
if [[ "$DRY_RUN" != "true" ]]; then
    echo "  [INFO] Refreshing stream metadata for LCR data..."
    snow sql -c "$CONNECTION_NAME" -q "
        USE DATABASE ${SOURCE_DATABASE:-AAA_DEV_SYNTHETIC_BANK};
        USE SCHEMA REP_RAW_001;
        SELECT SYSTEM\$STREAM_HAS_DATA('LIQI_RAW_SM_HQLA_FILES') AS hqla_stream_has_data,
               SYSTEM\$STREAM_HAS_DATA('LIQI_RAW_SM_DEPOSIT_FILES') AS deposit_stream_has_data;
    " > /dev/null 2>&1
fi

# =============================================================================
# WAIT FOR PARALLEL UPLOADS TO COMPLETE
# =============================================================================
if [[ $PARALLEL_THREADS -gt 1 ]]; then
    wait_all_uploads
    collect_parallel_results
fi

# =============================================================================
# UPLOAD SUMMARY
# =============================================================================
echo ""
echo "================================================================"
echo "=== UPLOAD SUMMARY"
echo "================================================================"
echo ""
echo "Statistics:"
echo "  Total Upload Operations: $TOTAL_UPLOADS"
echo "  Successful: $SUCCESSFUL_UPLOADS"
echo "  Failed: $FAILED_UPLOADS"

if [[ $TOTAL_UPLOADS -gt 0 ]]; then
    success_rate=$(( (SUCCESSFUL_UPLOADS * 100) / TOTAL_UPLOADS ))
    echo "  Success Rate: ${success_rate}%"
fi

echo ""

if [[ ${#FAILED_ITEMS[@]} -gt 0 ]]; then
    echo "[WARN] FAILED UPLOADS:"
    echo "================================================================"
    for item in "${FAILED_ITEMS[@]}"; do
        echo "  [FAIL] $item"
    done
    echo ""
    echo "Troubleshooting:"
    echo "  1. Check your network connection"
    echo "  2. Verify Snowflake credentials: snow connection test -c $CONNECTION_NAME"
    echo "  3. Check AWS S3 connectivity (Snowflake stages use S3)"
    echo "  4. Try increasing timeout: edit MAX_RETRIES and UPLOAD_TIMEOUT in script"
    echo "  5. Re-run the script - it uses OVERWRITE=TRUE to replace existing files"
    echo ""
fi

if [[ "$DRY_RUN" == "true" ]]; then
    echo "================================================================"
    echo "[DRY RUN] COMPLETED - No files were actually uploaded"
    echo "================================================================"
    echo ""
    echo "To execute the actual upload, run:"
    echo "  ./upload-data.sh --CONNECTION_NAME=$CONNECTION_NAME"
elif [[ $FAILED_UPLOADS -eq 0 ]]; then
    echo "================================================================"
    echo "[SUCCESS] ALL UPLOADS COMPLETED SUCCESSFULLY!"
    echo "================================================================"
    echo ""
    echo "Next steps:"
    echo "  1. Monitor task execution:"
    echo "     SHOW TASKS IN DATABASE ${SOURCE_DATABASE:-AAA_DEV_SYNTHETIC_BANK};"
    echo ""
    echo "  2. Check data loading:"
    echo "     SELECT COUNT(*) FROM [schema].[table];"
    echo ""
    echo "  3. Verify stage contents:"
    echo "     LIST @[stage_name];"
    echo ""
    echo "  4. Check streams with data:"
    echo "     SHOW STREAMS IN DATABASE ${SOURCE_DATABASE:-AAA_DEV_SYNTHETIC_BANK};"
    echo ""
    echo "Data is now ready for automated processing by Snowflake tasks!"
else
    echo "================================================================"
    echo "[WARN] UPLOAD COMPLETED WITH ERRORS"
    echo "================================================================"
    echo ""
    echo "Please review the failed uploads above and retry."
    echo ""
    exit 1
fi

echo ""
echo "Upload process completed!"
echo ""

# Cleanup temporary results directory
rm -rf "$RESULTS_DIR" 2>/dev/null || true
