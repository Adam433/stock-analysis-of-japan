import type { Metadata } from "next";
import { ErrorBoundary } from "@/components/shared/ErrorBoundary";
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
      <body>
        <a href="#main-content" className="skip-link">跳转到主要内容</a>
        <ErrorBoundary>{children}</ErrorBoundary>
      </body>
    </html>
  );
}
