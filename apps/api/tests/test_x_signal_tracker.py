from __future__ import annotations

import unittest
from datetime import UTC, date, datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import stockanalyse_api.domain.backtests.models  # noqa: F401
import stockanalyse_api.domain.fundamentals.models  # noqa: F401
import stockanalyse_api.domain.indicators.models  # noqa: F401
import stockanalyse_api.domain.instruments.models  # noqa: F401
import stockanalyse_api.domain.market_data.models  # noqa: F401
import stockanalyse_api.domain.operations.models  # noqa: F401
import stockanalyse_api.domain.screens.models  # noqa: F401
import stockanalyse_api.domain.watchlists.models  # noqa: F401
import stockanalyse_api.domain.x_signals.models  # noqa: F401
from stockanalyse_api.db.base import Base
from stockanalyse_api.domain.instruments.models import Instrument
from stockanalyse_api.domain.market_data.models import MarketDataDaily
from stockanalyse_api.services.x_signal_tracker import (
    ImportedXPost,
    XSignalLLMAnalysisItem,
    XSignalLLMPostAnalysis,
    add_x_signal_author,
    analyze_x_signal_author_posts,
    apply_x_signal_llm_analysis_results,
    create_x_signal_fetch_request,
    get_x_signal_dashboard,
    import_x_signal_posts,
    restore_x_signal_mention_llm_sentiment,
    update_x_signal_mention_sentiment,
)


class XSignalTrackerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        Base.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            future=True,
        )

    def tearDown(self) -> None:
        Base.metadata.drop_all(self.engine)
        self.engine.dispose()

    def _seed_us_stock(self, symbol: str = "NVDA") -> int:
        with self.session_factory() as session:
            instrument = Instrument(
                symbol=symbol,
                exchange="NASDAQ",
                name=f"{symbol} Corp",
                currency="USD",
            )
            session.add(instrument)
            session.flush()
            session.add_all(
                [
                    MarketDataDaily(
                        instrument_id=instrument.id,
                        trade_date=date(2026, 1, 2),
                        close=Decimal("100.000000"),
                    ),
                    MarketDataDaily(
                        instrument_id=instrument.id,
                        trade_date=date(2026, 5, 8),
                        close=Decimal("125.000000"),
                    ),
                ]
            )
            session.commit()
            return instrument.id

    def test_author_fetch_request_and_plain_post_import_are_persisted(self) -> None:
        with self.session_factory() as session:
            author = add_x_signal_author(session, "@SignalUser", display_name="Signal User")
            fetch_request = create_x_signal_fetch_request(
                session,
                author.id,
                lookback_months=20,
            )
            import_result = import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content="$NVDA bullish after earnings.",
                    )
                ],
            )
            dashboard = get_x_signal_dashboard(session)

        self.assertEqual(author.handle, "signaluser")
        self.assertEqual(fetch_request.status, "pending_chrome_capture")
        self.assertEqual(fetch_request.lookback_value, 20)
        self.assertEqual(import_result.created_count, 1)
        self.assertEqual(dashboard.total_posts, 1)
        self.assertEqual(dashboard.latest_fetch_request.status, "imported")

    def test_analysis_records_stock_sentiment_and_price_return(self) -> None:
        self._seed_us_stock()

        with self.session_factory() as session:
            author = add_x_signal_author(session, "signaluser")
            import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content="$NVDA bullish after earnings, still long.",
                    )
                ],
            )
            result = analyze_x_signal_author_posts(session, author.id)
            dashboard = get_x_signal_dashboard(session)
            mention = dashboard.mentions[0]

        self.assertEqual(result.mention_count, 1)
        self.assertEqual(mention.symbol, "NVDA")
        self.assertEqual(mention.sentiment, "unknown")
        self.assertIsNone(mention.llm_sentiment)
        self.assertIsNone(mention.manual_sentiment)
        self.assertEqual(mention.sentiment_source, "extraction")
        self.assertEqual(mention.mention_close, "100.000000")
        self.assertEqual(mention.latest_close, "125.000000")
        self.assertEqual(mention.cumulative_return, "0.25")
        self.assertEqual(mention.post_id, 1)
        self.assertEqual(mention.author_handle, "signaluser")
        self.assertEqual(mention.exchange, "NASDAQ")
        self.assertEqual(mention.company_name, "NVDA Corp")
        self.assertEqual(mention.is_sector_proxy, False)
        self.assertEqual(mention.mention_kind, "stock")
        self.assertEqual(mention.id, 1)
        self.assertEqual(mention.author_id, author.id)
        self.assertEqual(mention.confidence, "0.0000")
        self.assertEqual(mention.analysis_source, "extraction-v1")
        self.assertEqual(mention.mention_date, "2026-01-02")
        self.assertEqual(mention.mention_count, 1)
        self.assertEqual(mention.source_post_ids, [1])
        self.assertEqual(mention.mention_price_date, "2026-01-02")
        self.assertEqual(mention.latest_price_date, "2026-05-08")
        self.assertEqual(mention.source_text_excerpt, "$NVDA bullish after earnings, still long.")

    def test_sector_only_post_waits_for_llm_instead_of_keyword_proxy(self) -> None:
        self._seed_us_stock()

        with self.session_factory() as session:
            author = add_x_signal_author(session, "macroreader")
            import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content="AI 算力需求继续突破，整个板块还是看多。",
                    )
                ],
            )
            result = analyze_x_signal_author_posts(session, author.id)
            dashboard = get_x_signal_dashboard(session)

        self.assertEqual(result.mention_count, 0)
        self.assertEqual(dashboard.total_mentions, 0)

    def test_explicit_company_name_records_stock_without_direction_guess(self) -> None:
        self._seed_us_stock("QCOM")

        with self.session_factory() as session:
            author = add_x_signal_author(session, "chipreader")
            import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content="高通跑分用16w的手机功耗，市场还需要重新评估。",
                    )
                ],
            )
            result = analyze_x_signal_author_posts(session, author.id)
            mention = get_x_signal_dashboard(session).mentions[0]

        self.assertEqual(result.mention_count, 1)
        self.assertEqual(mention.symbol, "QCOM")
        self.assertEqual(mention.mention_kind, "stock")
        self.assertEqual(mention.sentiment, "unknown")

    def test_goog_mentions_use_googl_price_data_when_goog_instrument_is_absent(self) -> None:
        self._seed_us_stock("GOOGL")

        with self.session_factory() as session:
            author = add_x_signal_author(session, "bigtech")
            import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content="$GOOG and 谷歌 are both Alphabet exposure here.",
                    )
                ],
            )
            analyze_x_signal_author_posts(session, author.id)
            mention = get_x_signal_dashboard(session).mentions[0]

        self.assertEqual(mention.symbol, "GOOG")
        self.assertEqual(mention.exchange, "NASDAQ")
        self.assertEqual(mention.company_name, "GOOGL Corp")
        self.assertEqual(mention.mention_close, "100.000000")
        self.assertEqual(mention.latest_close, "125.000000")

    def test_analysis_keeps_full_source_text_for_details(self) -> None:
        self._seed_us_stock()
        long_content = "$NVDA " + ("半导体利润链条正在重构。" * 40) + "FULL_TEXT_TAIL"

        with self.session_factory() as session:
            author = add_x_signal_author(session, "longform")
            import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content=long_content,
                    )
                ],
            )
            analyze_x_signal_author_posts(session, author.id)
            mention = get_x_signal_dashboard(session).mentions[0]

        self.assertIn("FULL_TEXT_TAIL", mention.source_text_excerpt or "")
        self.assertEqual(mention.source_text_excerpt, long_content)

    def test_same_symbol_same_day_is_aggregated_into_one_mention(self) -> None:
        self._seed_us_stock()

        with self.session_factory() as session:
            author = add_x_signal_author(session, "signaluser")
            import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 10, 0, tzinfo=UTC),
                        content="$NVDA bullish in the morning.",
                    ),
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content="$NVDA still long into the close.",
                    ),
                ],
            )
            result = analyze_x_signal_author_posts(session, author.id)
            dashboard = get_x_signal_dashboard(session)
            mention = dashboard.mentions[0]

        self.assertEqual(result.mention_count, 1)
        self.assertEqual(dashboard.total_mentions, 1)
        self.assertEqual(mention.symbol, "NVDA")
        self.assertEqual(mention.mention_date, "2026-01-02")
        self.assertEqual(mention.mention_count, 2)
        self.assertEqual(mention.sentiment, "unknown")
        self.assertEqual(mention.source_post_ids, [1, 2])

    def test_unprefixed_uppercase_words_are_not_treated_as_tickers(self) -> None:
        self._seed_us_stock("COST")

        with self.session_factory() as session:
            author = add_x_signal_author(session, "signaluser")
            import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content="This is not a news FEED. Different COST bases, same mechanics.",
                    )
                ],
            )
            analyze_x_signal_author_posts(session, author.id)
            dashboard = get_x_signal_dashboard(session)

        self.assertEqual(dashboard.total_mentions, 0)

    def test_reanalysis_replaces_prior_heuristic_mentions_without_duplicate_rows(self) -> None:
        self._seed_us_stock()

        with self.session_factory() as session:
            author = add_x_signal_author(session, "signaluser")
            import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content="$NVDA bullish.",
                    )
                ],
            )
            first = analyze_x_signal_author_posts(session, author.id)
            second = analyze_x_signal_author_posts(session, author.id)
            dashboard = get_x_signal_dashboard(session)

        self.assertEqual(first.mention_count, 1)
        self.assertEqual(second.mention_count, 1)
        self.assertEqual(dashboard.total_mentions, 1)

    def test_llm_analysis_records_sector_proxies_and_marks_posts(self) -> None:
        self._seed_us_stock("NVDA")
        self._seed_us_stock("AVGO")

        with self.session_factory() as session:
            author = add_x_signal_author(session, "macroreader")
            import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content="半导体板块利润率继续上修，AI 基建需求还是看多。",
                    )
                ],
            )
            result = apply_x_signal_llm_analysis_results(
                session,
                [
                    XSignalLLMPostAnalysis(
                        post_id=1,
                        items=[
                            XSignalLLMAnalysisItem(
                                symbol="NVDA",
                                sentiment="bullish",
                                mention_kind="sector_proxy",
                                sector_label="semiconductors",
                                confidence=Decimal("0.8000"),
                                reason="全文讨论半导体板块并看多。",
                                is_sector_proxy=True,
                                proxy_reason="半导体板块龙头之一。",
                            ),
                            XSignalLLMAnalysisItem(
                                symbol="AVGO",
                                sentiment="bullish",
                                mention_kind="sector_proxy",
                                sector_label="semiconductors",
                                confidence=Decimal("0.8000"),
                                reason="全文讨论半导体板块并看多。",
                                is_sector_proxy=True,
                                proxy_reason="半导体板块龙头之一。",
                            ),
                        ],
                    )
                ],
                analysis_source="5.5ExtraHigh",
            )
            dashboard = get_x_signal_dashboard(session)
            post = session.get(stockanalyse_api.domain.x_signals.models.XSignalPost, 1)

        self.assertEqual(result.analysis_source, "5.5ExtraHigh")
        self.assertEqual(result.mention_count, 2)
        self.assertEqual([mention.symbol for mention in dashboard.mentions], ["AVGO", "NVDA"])
        for mention in dashboard.mentions:
            self.assertEqual(mention.mention_kind, "sector_proxy")
            self.assertEqual(mention.sector_label, "semiconductors")
            self.assertEqual(mention.sentiment, "bullish")
            self.assertEqual(mention.analysis_source, "5.5ExtraHigh")
            self.assertEqual(mention.llm_sentiment, "bullish")
            self.assertIsNone(mention.manual_sentiment)
            self.assertEqual(mention.sentiment_source, "llm")
            self.assertTrue(mention.is_sector_proxy)
        self.assertIsNotNone(post)
        self.assertIn("x_signal_llm_analysis", post.raw_payload_json or "")

    def test_llm_analysis_marks_posts_with_no_items(self) -> None:
        with self.session_factory() as session:
            author = add_x_signal_author(session, "macroreader")
            import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content="今天只是闲聊，没有投资观点。",
                    )
                ],
            )
            result = apply_x_signal_llm_analysis_results(
                session,
                [XSignalLLMPostAnalysis(post_id=1, items=[])],
                analysis_source="5.4mini-High",
            )
            dashboard = get_x_signal_dashboard(session)
            post = session.get(stockanalyse_api.domain.x_signals.models.XSignalPost, 1)

        self.assertEqual(result.analyzed_posts, 1)
        self.assertEqual(result.mention_count, 0)
        self.assertEqual(dashboard.total_mentions, 0)
        self.assertIsNotNone(post)
        self.assertIn("5.4mini-High", post.raw_payload_json or "")

    def test_llm_analysis_accepts_unable_to_determine_label(self) -> None:
        self._seed_us_stock("NVDA")

        with self.session_factory() as session:
            author = add_x_signal_author(session, "macroreader")
            import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content="$NVDA is mentioned in a mixed market note.",
                    )
                ],
            )
            apply_x_signal_llm_analysis_results(
                session,
                [
                    XSignalLLMPostAnalysis(
                        post_id=1,
                        items=[
                            XSignalLLMAnalysisItem(
                                symbol="NVDA",
                                sentiment="无法判断",
                                confidence=Decimal("0.4000"),
                                reason="原文提到个股但方向不明确。",
                            )
                        ],
                    )
                ],
                analysis_source="5.5ExtraHigh",
            )
            mention = get_x_signal_dashboard(session).mentions[0]

        self.assertEqual(mention.sentiment, "unknown")
        self.assertEqual(mention.llm_sentiment, "unknown")
        self.assertIsNone(mention.manual_sentiment)
        self.assertEqual(mention.sentiment_source, "llm")

    def test_manual_sentiment_override_can_restore_llm_judgment(self) -> None:
        self._seed_us_stock("NVDA")

        with self.session_factory() as session:
            author = add_x_signal_author(session, "macroreader")
            import_x_signal_posts(
                session,
                author.id,
                [
                    ImportedXPost(
                        posted_at=datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
                        content="$NVDA demand is softening.",
                    )
                ],
            )
            apply_x_signal_llm_analysis_results(
                session,
                [
                    XSignalLLMPostAnalysis(
                        post_id=1,
                        items=[
                            XSignalLLMAnalysisItem(
                                symbol="NVDA",
                                sentiment="bearish",
                                confidence=Decimal("0.8000"),
                                reason="原文表达需求转弱。",
                            )
                        ],
                    )
                ],
                analysis_source="5.5ExtraHigh",
            )
            mention_id = get_x_signal_dashboard(session).mentions[0].id

            overridden = update_x_signal_mention_sentiment(session, mention_id, "看多")
            restored = restore_x_signal_mention_llm_sentiment(session, mention_id)

        self.assertEqual(overridden.sentiment, "bullish")
        self.assertEqual(overridden.llm_sentiment, "bearish")
        self.assertEqual(overridden.manual_sentiment, "bullish")
        self.assertEqual(overridden.sentiment_source, "manual")
        self.assertEqual(restored.sentiment, "bearish")
        self.assertEqual(restored.llm_sentiment, "bearish")
        self.assertIsNone(restored.manual_sentiment)
        self.assertEqual(restored.sentiment_source, "llm")


if __name__ == "__main__":
    unittest.main()
