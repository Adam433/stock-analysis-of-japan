import path from "node:path";
import { fileURLToPath } from "node:url";
import type { NextConfig } from "next";

const currentDir = path.dirname(fileURLToPath(import.meta.url));
const workspaceRoot = path.resolve(currentDir, "../..");

const nextConfig: NextConfig = {
  turbopack: {
    root: workspaceRoot
  },
  webpack: (config) => {
    config.resolve = config.resolve ?? {};
    config.resolve.modules = [
      ...(config.resolve.modules ?? []),
      path.resolve(workspaceRoot, "node_modules"),
    ];
    return config;
  },
};

export default nextConfig;
