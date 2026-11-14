import path from 'path';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: false,
    environment: 'node',
    include: ['__tests__/**/*.test.ts'],
    // local-ci/unit-tests.sh: JUnit for test history, lcov for SonarQube. Kept out of test-results/,
    // which Playwright empties on every run.
    reporters: process.env.CI_REPORTS ? ['default', 'junit'] : ['default'],
    outputFile: { junit: 'reports/junit-unit.xml' },
    coverage: {
      provider: 'v8',
      reporter: ['text-summary', 'lcov'],
      reportsDirectory: 'reports/coverage',
      include: ['lib/**/*.ts', 'app/api/**/*.ts'],
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(import.meta.dirname, './'),
    },
  },
});
