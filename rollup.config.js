import typescript from "@rollup/plugin-typescript";
import { nodeResolve } from "@rollup/plugin-node-resolve";
import commonjs from "@rollup/plugin-commonjs";
import packageJson from "./package.json";
import PeerDepsExternalPlugin from "rollup-plugin-peer-deps-external";
import dts from "rollup-plugin-dts";
import terser from "@rollup/plugin-terser";

const config = [
  {
    input: "src/index.ts",
    output: [
      {
        file: packageJson.main,
        format: "cjs",
        sourcemap: true,
      },
      {
        file: packageJson.module,
        format: "esm",
        sourcemap: true,
      },
    ],
    external: ["@langchain/langgraph-checkpoint", "@langchain/core", "redis"],
    plugins: [
      PeerDepsExternalPlugin(),
      terser(),
      nodeResolve({ preferBuiltins: true, browser: false }),
      typescript({
        tsconfig: "./tsconfig.json",
        declaration: true,
        declarationDir: "./dist",
        rootDir: "./src",
        exclude: ["**/*.test.ts", "examples/**/*"],
      }),
      commonjs(),
    ],
  },
  {
    input: "src/index.ts",
    output: [
      {
        file: packageJson.types,
      },
    ],
    plugins: [dts.default()],
  },
];

export default config;
