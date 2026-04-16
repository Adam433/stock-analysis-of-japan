"use client";

import { Component, type ReactNode } from "react";

type ErrorBoundaryProps = {
  children: ReactNode;
  fallback?: ReactNode;
};

type ErrorBoundaryState = {
  hasError: boolean;
  error: Error | null;
};

export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  render() {
    if (this.state.hasError) {
      if (this.props.fallback) {
        return this.props.fallback;
      }

      return (
        <section className="error-boundary-fallback" role="alert">
          <h2>页面渲染出错</h2>
          <p className="status-copy">
            {this.state.error?.message ?? "发生了未知错误，请刷新页面重试。"}
          </p>
          <button
            type="button"
            className="strategy-button"
            onClick={() => this.setState({ hasError: false, error: null })}
          >
            重试
          </button>
        </section>
      );
    }

    return this.props.children;
  }
}
