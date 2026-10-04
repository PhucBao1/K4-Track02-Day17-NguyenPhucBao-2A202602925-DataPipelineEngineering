# Bonus B2 — Pipeline feature chống gian lận cho ví điện tử

**Học viên:** Nguyễn Phúc Bảo / 2A202602925

## 1. Bài toán và ràng buộc

Một ví điện tử Việt Nam (~3 triệu người dùng hoạt động, ~1,5 triệu giao dịch/ngày,
đỉnh ~300 giao dịch/giây vào ngày lương và 11/11) cần một **mô hình chấm điểm gian lận**
cho mỗi giao dịch chuyển tiền / thanh toán. Mô hình phải trả lời trong **< 150 ms**
(nằm trên đường thanh toán), và đội rủi ro retrain hằng tuần.

Vì sao khó:

- **Nhãn đến muộn và bẩn.** Một giao dịch chỉ được xác nhận là gian lận khi khách khiếu
  nại hoặc ngân hàng gửi chargeback — trễ từ 1 đến 45 ngày. Phần lớn giao dịch không bao
  giờ có nhãn rõ ràng.
- **Rò rỉ tương lai là thảm hoạ.** Feature kiểu "số giao dịch của thiết bị trong 24 giờ"
  nếu tính bằng dữ liệu *sau* thời điểm giao dịch sẽ làm mô hình offline đẹp (AUC 0,97)
  nhưng ngoài đời vô dụng.
- **Nguồn đa dạng:** Postgres core-banking (CDC), sự kiện app (Kafka), log thiết bị/IP,
  danh sách đen từ đối tác, và quyết định thủ công của nhân viên rủi ro.
- **Dữ liệu cá nhân nhạy cảm** (CCCD, số tài khoản, vị trí) — chịu Nghị định 13/2023
  về bảo vệ dữ liệu cá nhân.

## 2. Các câu hỏi then chốt và quyết định

### Q2 — Batch hay streaming?

**Quyết định:** kiến trúc **hai tốc độ, một định nghĩa feature**. Feature "nóng"
(đếm giao dịch theo thiết bị/IP/người nhận trong 5 phút, 1 giờ, 24 giờ) tính bằng
streaming (Flink đọc Kafka) và ghi vào online store (Redis). Feature "lạnh" (tuổi tài
khoản, lịch sử 90 ngày, đồ thị người nhận) tính batch hằng đêm bằng dbt trên lakehouse.

**Đánh đổi:** streaming toàn bộ (Kappa) vs. batch toàn bộ. Batch toàn bộ rẻ và dễ đúng
nhưng gian lận kiểu "chiếm tài khoản rồi rút sạch trong 10 phút" chỉ bắt được bằng
feature tính trong vài giây. Streaming toàn bộ thì đắt và khó backfill 90 ngày lịch sử.
Chọn lai vì chỉ ~15% feature thật sự cần độ tươi dưới một phút.

### Q5 — Train/serve parity và point-in-time

**Quyết định:** mọi feature được định nghĩa **một lần** (SQL/feature view có
`event_time`), và training set được dựng bằng **ASOF join** theo `transaction_time`:
mỗi giao dịch chỉ "thấy" giá trị feature có `valid_from <= transaction_time`. Online
store log lại vector feature *thực sự* đã dùng khi chấm điểm (feature logging), để có
thể so offline vs. online hằng ngày.

**Đánh đổi:** tính lại feature từ log thô cho training (đúng point-in-time nhưng tốn
compute) vs. dùng feature đã log lúc serve (rẻ, khớp 100% với serve, nhưng không thử
được feature mới trên quá khứ). Tôi dùng **cả hai**: feature logging làm nguồn sự thật
cho mô hình hiện tại, backfill ASOF cho feature mới — và một job parity cảnh báo khi hai
cách lệch quá 1%.

### Q4 — Hợp đồng dữ liệu và quarantine

**Quyết định:** hợp đồng schema (Pydantic/Avro + schema registry) ở cửa vào Kafka; bản
ghi sai đi vào topic `quarantine` kèm lý do, **không chặn** pipeline. Giám sát *tỷ lệ*
quarantine theo nguồn: tăng > 0,5% trong 15 phút thì báo on-call của đội nguồn, không
phải đội dữ liệu.

**Đánh đổi:** fail-fast (dừng pipeline khi có bản ghi xấu) vs. quarantine. Với đường
thanh toán, dừng pipeline = feature cũ = gian lận lọt qua, nên quarantine thắng; cái giá
là phải có người thật sự xem quarantine.

### Q7 — Flywheel nhãn mà không tự đầu độc

**Quyết định:** nhãn được version hoá giống snapshot training của lab: `label_snapshot =
v<ngày>` chỉ chứa nhãn đã biết **tại ngày đó**, và mỗi giao dịch chỉ được dùng làm mẫu âm
sau "cửa sổ chín" 45 ngày. Quyết định chặn của chính mô hình **không** được dùng làm nhãn
dương (vòng lặp tự xác nhận); giữ 1–2% giao dịch rủi ro thấp đi qua ngẫu nhiên để đo
precision thật.

**Đánh đổi:** dùng nhãn ngay (mô hình tươi hơn nhưng học rằng "chưa khiếu nại = sạch")
vs. đợi chín 45 ngày (mô hình chậm hơn ~6 tuần). Chọn đợi chín cho mẫu âm, nhưng dùng
ngay mẫu dương đã xác nhận — gian lận đã xác nhận không cần đợi.

### Q10 — Bối cảnh Việt Nam: dữ liệu cá nhân và quyền xoá

**Quyết định:** CCCD, số tài khoản, số điện thoại được **token hoá** ngay ở Bronze
(HMAC với khoá trong KMS); mọi tầng sau chỉ thấy token, vẫn join và đếm được. Văn bản tự do
(ghi chú chuyển khoản) chạy qua bộ che PII tiếng Việt (regex + NER) trước Silver. Yêu cầu
xoá → tombstone ở Silver (như T-97 trong lab) + **crypto-shredding** khoá theo người
dùng, để snapshot training cũ trở thành không đọc được mà không phải sửa file bất biến.

**Đánh đổi:** xoá vật lý trong mọi snapshot (đúng tuyệt đối, nhưng phá tính tái lập
và tốn kém) vs. crypto-shredding (rẻ, giữ checksum cấu trúc, nhưng phụ thuộc quản lý khoá
chặt). Chọn crypto-shredding, kèm quy định: Nghị định 13 yêu cầu lưu hồ sơ giao dịch
cho mục đích chống rửa tiền, nên **dữ liệu giao dịch** (đã token hoá) được giữ theo hạn
luật định, còn **dữ liệu định danh** thì bị xoá.

## 3. Phương án bị loại

**Loại: tính toàn bộ feature trực tiếp trong service chấm điểm** (query Postgres
production lúc nhận giao dịch, `SELECT count(*) ... WHERE device_id = ? AND ts > now() -
interval '24h'`). Ưu điểm hấp dẫn: không cần streaming, luôn tươi, không lo parity vì
chỉ có một code path. Bị loại vì: (1) ở 300 giao dịch/giây, mỗi giao dịch 20–30 query
aggregate sẽ đè database core-banking — đúng hệ thống không được phép chậm; (2) không
thể dựng training set point-in-time vì query dùng `now()`, không có event time — tức là
lặp lại đúng lỗi lookback của lab nhưng ở quy mô tiền thật; (3) p99 latency không kiểm
soát được.

## 4. Kiến trúc

```
Postgres core-banking ──Debezium CDC──┐
App events (Kafka) ───────────────────┼──▶ Bronze (Parquet/Iceberg, bất biến, PII token hoá)
Log thiết bị/IP, blacklist đối tác ───┘          │                         │
                                                 │ dbt nightly             │ Flink (streaming)
                                                 ▼                         ▼
                                    Silver: giao dịch, tài khoản    feature nóng 5m/1h/24h
                                    (MERGE theo khoá + LSN guard)          │
                                                 │                         ▼
                                    Gold: feature lạnh 90 ngày ──▶ Online store (Redis) ──▶ Scoring API (<150ms)
                                                 │                                              │
                                    ASOF join theo transaction_time ◀── feature logging ◀───────┘
                                                 ▼
                         gold_training_set v<ngày> (nhãn chín 45 ngày) ──▶ retrain hằng tuần
                                                 │
                                       parity job offline vs online (> 1% → cảnh báo)
```

## 5. Liên hệ với lab

Lab đã cho tôi thấy ba lỗi này ở dạng thu nhỏ: thiếu khoá (MERGE + LSN), dữ liệu muộn
(lookback = ceil(P99), ở đây là "cửa sổ chín" của nhãn), và xoá phải lan (tombstone).
Trong bài toán gian lận, cùng ba nguyên tắc đó quyết định giữa một mô hình chạy được và
một mô hình chỉ đẹp trên notebook. Điểm khác biệt lớn nhất là **chi phí sai**: ở lab sai
là lệch checksum, ở đây sai là mất tiền thật của khách hàng.
