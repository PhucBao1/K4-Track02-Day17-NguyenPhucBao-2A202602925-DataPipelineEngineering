# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Phúc Bảo / 2A202602925
**Repo:** https://github.com/PhucBao1/K4-Track02-Day17-NguyenPhucBao-2A202602925-DataPipelineEngineering
**Commit bài nộp:** `a97d2f2` (sửa 3 lỗi trong `pipeline/staging.py`, `pipeline/silver.py`, `pipeline/config.py` + bonus)
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Claude Code (Claude Opus 5.5) — đọc code, chạy lệnh, đề xuất cách sửa và nháp report; tôi đã đọc lại và giải thích được từng dòng sửa.
**Nguồn tham khảo khác (nếu có):** Slide Day 17; tài liệu Debezium (event envelope), DuckDB `MERGE INTO`.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Verify: `silver_tickets` có **24 rows for 12 tickets**; T-91 có 3 hàng (`low/open`, `high/open`, `high/closed/bug`); rerun lệch checksum | `gold_feature_daily` ≠ full recompute (`c50b8851…` ≠ `8630e04a…`); u05 ngày 08-12 đếm `(2, 0)` thay vì `(5, 1)`; `LOOKBACK_DAYS=0 < p99=3` | T-97 vẫn `is_deleted = false` với `user_id`, `subject`, `body`; còn trong snapshot `v2026-08-16` và 2 chunk trong `gold_doc_chunks` |
| **Nguyên nhân gốc** | `upsert_silver_tickets` dùng `INSERT INTO` — mỗi batch (và mỗi lần chạy lại) **append** thêm hàng, không có khoá | Lookback 0 ngày: run ngày D chỉ tính lại partition D, nên event 08-12 đến ngày 08-15 nằm trong Silver nhưng partition 08-12 không bao giờ được tính lại | `ticket_changes_sql` lấy `ticket_id` từ `after`; với `op='d'` thì `after = null` → `ticket_id` NULL → bị `WHERE ticket_id IS NOT NULL` loại; xoá không bao giờ tới Silver |
| **Cách sửa** | `pipeline/silver.py`: `MERGE INTO silver_tickets ON ticket_id`, `WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT` | `pipeline/config.py`: `LOOKBACK_DAYS = 3` = ceil(P99) đo bằng `main.py --lateness` | `pipeline/staging.py`: `coalesce(after->>'ticket_id', before->>'ticket_id')`; cột PII vẫn đọc từ `after` nên thành NULL ⇒ tombstone |
| **Khái niệm trên slide** | Silver — có khoá; MERGE theo khoá + LSN guard (idempotent, batch cũ không đè trạng thái mới) | Data về muộn: xử lý theo event time, lookback = ceil(P99) đo từ Bronze | CDC log-based (phong bì Debezium `before/after/op`); "Xoá phải lan" xuống Silver → training set → RAG |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày (p50 = 0, p95 = 2.90, max = 3, n = 43) → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY** (`silver_tickets` 3c15dfd43701, `gold_feature_daily` 8630e04a61d1)

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- **MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`:** Silver là bảng thực thể (1 hàng = 1 ticket) nhận thay đổi lẻ tẻ theo CDC nên cần upsert theo khoá với LSN làm thứ tự; còn feature daily là aggregate theo ngày, tính lại cả partition `[D−3, D]` từ Silver vừa đơn giản vừa tự idempotent.
- **Tombstone thay vì xoá hẳn hàng trong Silver:** giữ hàng `is_deleted = true` (PII đã xoá trắng) cùng `_lsn` để MERGE vẫn so được LSN — nếu xoá hẳn, chạy lại batch cũ sẽ `INSERT` lại T-97; đồng thời downstream (RAG, training) có tín hiệu rõ để loại ticket. Đánh đổi: hàng tombstone tồn tại mãi (cần job compaction định kỳ).
- **Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ:** để mô hình đã train trên `v2026-08-12` luôn tái lập được (point-in-time, không rò dữ liệu tương lai); sửa dữ liệu thì tạo version mới.
- **DuckDB (lite) / dbt (track dbt) thay vì Spark:** dữ liệu vài chục KB/ngày, chạy một máy trong vài giây; Spark chỉ thêm chi phí cluster/khởi động. dbt cho cùng logic dạng SQL có test, contract, `merge`/`microbatch` — dễ đối chiếu (parity).

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot bất biến vs. quyền được xoá.** Quyền xoá (luật) thắng. Tôi tách PII khỏi snapshot: snapshot chỉ lưu `ticket_id` + văn bản đã che, còn văn bản gốc mã hoá bằng khoá riêng theo user (crypto-shredding) — xoá khoá ⇒ mọi snapshot cũ không đọc được nội dung của T-97 mà checksum cấu trúc vẫn giữ. Nếu không có hạ tầng đó: tạo lại `v2026-08-12..14` từ Bronze đã loại T-97, ghi log "re-issued do yêu cầu xoá", retrain mô hình nếu cần, và xoá/redact cả bản Bronze gốc.
2. **PII còn sót (tên người).** Đặt chốt ngay khi rời Bronze (staging → Silver): regex + NER (ví dụ mô hình NER tiếng Việt / Presidio) thay tên bằng `<NAME>`; Bronze bị giới hạn quyền truy cập và có TTL. Đo bằng một tập ticket có gán nhãn PII thủ công: recall (tỷ lệ PII bị che) là chỉ số chính, kèm một check trong `verify` quét Silver/Gold bằng danh sách tên đã biết, fail run nếu recall < ngưỡng.

## 5. Output (dán nguyên văn)

Chạy trên Windows PowerShell (`$env:PYTHONIOENCODING='utf-8'` để in được tiếng Việt).

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 2.73s

$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ .\.venv\Scripts\python.exe main.py --land-only
$ cd dbt_project; ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Running with dbt=1.12.5
Registered adapter: duckdb=1.11.0
Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
Concurrency: 1 threads (target='dev')
1 of 19 OK created sql view model main.stg_events
2 of 19 OK created sql view model main.stg_ticket_changes
3 of 19 OK created sql incremental model main.silver_events
4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone
8 of 19 OK created sql incremental model main.silver_tickets
5..15 of 19 PASS (not_null / unique / accepted_values on silver_events, silver_tickets)
16 of 19 OK created sql microbatch model main.gold_feature_daily (Batch 1..7 of 7: 2026-08-10 .. 2026-08-16 OK)
17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date
18 of 19 PASS not_null_gold_feature_daily_event_date
19 of 19 PASS not_null_gold_feature_daily_user_id
Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 9.68 seconds (9.68s).
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

Baseline trước khi sửa (bản clone): `RESULT: 8/18 checks — FAILURES ABOVE`, pytest 9 failed.

### Bonus

**B1 — LLM step có cache** (`pipeline/llm_label.py`, commit `a97d2f2`): cache `llm_label_cache`
khoá bằng `sha256(text) + model + prompt_version`, lưu cả câu trả lời sai để chạy lại vẫn 0 lần gọi;
Gold chỉ nhận nhãn thuộc `bug/billing/other`, phần còn lại vào `llm_label_quarantine`;
ước tính token/chi phí cho phần chưa có trong cache trước khi gọi model.

```text
$ .\.venv\Scripts\python.exe -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

**B2 — Brainstorm:** [`bonus/DESIGN.md`](../bonus/DESIGN.md) — pipeline feature chống gian lận
cho ví điện tử (5 câu hỏi then chốt, đánh đổi, một phương án bị loại, sơ đồ kiến trúc).
