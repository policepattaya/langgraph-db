import typescript from "@rollup/plugin-typescript";
import { nodeResolve } from "@rollup/plugin-node-resolve";
import commonjs from "@rollup/plugin-commonjs";
import json from "@rollup/plugin-json";

const config = [
  {
    input: "src/index.ts",
    output: [
      {
        file: "dist/index.js",
        format: "es",
        sourcemap: true,
      },
      {
        file: "dist/index.cjs",
        format: "cjs",
        sourcemap: true,
      },
      {
        file: "dist/index.d.ts",
        format: "es",
        sourcemap: true,
      },
    ],
    external: [
      "@langchain/langgraph-checkpoint",
      "@langchain/core",
      "redis",
    ],
    plugins: [
      nodeResolve({ preferBuiltins: true, browser: false }),
      typescript({
        tsconfig: "./tsconfig.json",
        declaration: true,
        declarationDir: "./dist",
        rootDir: "./src",
        exclude: ["**/*.test.ts", "examples/**/*"],
      }),
      commonjs(),
      json(),
    ],
  },
];

export default config; 