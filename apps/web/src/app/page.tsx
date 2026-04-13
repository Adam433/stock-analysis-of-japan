const workflowSteps = [
  "Initialize data ingestion and normalization",
  "Materialize RPS and 52-week-high facts",
  "Screen Japan equities and inspect chart details"
];

export default function HomePage() {
  return (
    <main className="min-h-screen px-6 py-12">
      <section className="mx-auto flex max-w-5xl flex-col gap-10 rounded-[2rem] border border-black/10 bg-white/75 p-10 shadow-[0_24px_80px_rgba(31,26,20,0.08)] backdrop-blur">
        <div className="flex flex-col gap-4">
          <p className="text-sm uppercase tracking-[0.3em] text-[var(--accent)]">
            stockAnalyse
          </p>
          <h1 className="max-w-3xl text-5xl leading-tight">
            Research shell for Japan equity screening, chart review, and backtesting.
          </h1>
          <p className="max-w-2xl text-lg leading-8 text-[var(--muted)]">
            This is the initial application scaffold. Future stories will attach
            screening flows, chart detail views, watchlists, and backtest results
            to this shell.
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-3">
          {workflowSteps.map((step, index) => (
            <article
              key={step}
              className="rounded-[1.5rem] border border-black/10 bg-[#fffdf8] p-6"
            >
              <p className="text-sm uppercase tracking-[0.2em] text-[var(--accent)]">
                Step {index + 1}
              </p>
              <p className="mt-3 text-xl leading-8">{step}</p>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}
