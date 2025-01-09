import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  /* config options here */
  webpack: (config, { /*isServer,*/ dev }) => {
    // Treat @dbos-inc/dbos-sdk as an external package for client builds
    config.externals = [
      ...config.externals,
      {
        "@dbos-inc/dbos-sdk": "commonjs @dbos-inc/dbos-sdk",
      },
    ];

    if (dev || true) {
      config.optimization.minimize = false;
    }

    return config;
  },
};

export default nextConfig;
