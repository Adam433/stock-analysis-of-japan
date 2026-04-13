import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "stockAnalyse",
  description: "日股筛选与回测工作台"
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="zh-CN">
      <body>{children}</body>
    </html>
  );
}
