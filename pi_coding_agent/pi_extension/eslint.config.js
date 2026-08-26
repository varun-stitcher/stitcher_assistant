// ESLint flat config for the stitcher-pi extension.
// - index.ts   -> type-aware-ish linting via typescript-eslint (no type info)
// - mcpClient.mjs -> plain JS via @eslint/js recommended
// prettier runs last to turn off any stylistic rules it owns (formatting is
// prettier's job; this config only reports code-smell / correctness issues).
import js from '@eslint/js';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  { ignores: ['node_modules/**', 'dist/**', 'package-lock.json'] },
  // Shared bases: recommended JS + recommended TS (untyped).
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ['**/*.{ts,mjs}'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: {
        process: 'readonly',
        console: 'readonly',
        URL: 'readonly',
      },
    },
    rules: {
      // ---- optional strictness tweaks for this thin wrapper ----
      '@typescript-eslint/no-explicit-any': 'warn',
      '@typescript-eslint/no-unused-vars': ['warn', { argsIgnorePattern: '^_' }],
      'no-unused-vars': 'off',
    },
  }
);
