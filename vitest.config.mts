import { defineConfig } from 'vitest/config';

export default defineConfig({
  root: '.',
  test: {
    globalSetup: './test/helper/test-setup.ts',
    setupFiles: ['./test/helper/expectations.ts'],
    include: ['test/core/**/*.test.{ts,js}'],
    environment: 'node',
    poolOptions: {
      threads: {
        singleThread: true,
      },
    },
  },
});
