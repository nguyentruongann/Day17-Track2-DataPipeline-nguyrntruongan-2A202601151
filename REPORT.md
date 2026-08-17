# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Trường An  **Lớp:** E403  **Ngày:** 2026-08-17

---

## 0 · Kết quả cuối

Sau khi sửa, bộ tiêu chí cần đạt là:

```text
gold_training_set     12,480 / 12,480   ổn định ✓
gold_feature_daily     9,100 /  9,100   ổn định ✓
gold_doc_chunks       31,200 / 31,200   ổn định ✓
quarantine_tickets       312 /    312   ổn định ✓
dbt test              11/11 pass
DAG                    catchup=False / max_active_runs=1
Tổng kết               4/4 tiêu chí đạt
```

Checksum đối chiếu trên bộ seed deterministic của Lab:

```text
gold_training_set     8dd7c98653
gold_feature_daily    3db448685c
gold_doc_chunks       92d8e50131
quarantine_tickets    ebb89036fb
```

> Khi nộp bài, chạy `make verify` trên máy của bạn và dán nguyên output thực tế vào đây nếu giảng viên yêu cầu log đầy đủ thời gian từng lượt chạy.

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` tăng số hàng sau mỗi lần chạy lại; trạng thái ban đầu có 38.750 hàng trong khi kỳ vọng 12.480, cùng một `ticket_id` bị lặp. |
| **Nguyên nhân** | Model là `incremental` nhưng không khai báo `unique_key` và chiến lược ghi, nên dbt ghi kiểu append/`INSERT`. Retry cùng dữ liệu vì vậy ghi thêm thay vì thay thế. Đồng thời nguồn CDC có `op='u'`, nên cùng một entity ticket có thể xuất hiện ở nhiều ngày; cách xoá/ghi theo partition ngày không giải quyết đúng grain entity. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key='ticket_id'` và `incremental_strategy='merge'`. `dags/ai_training_pipeline.py`: đặt `catchup=False`, `max_active_runs=1` để tránh backfill/chạy song song làm tăng khả năng kích hoạt lỗi. |
| **Bằng chứng** | Trước: 38.750 hàng, có ticket lặp. Sau: 12.480 hàng, 1 hàng / 1 ticket, checksum ổn định qua các lượt chạy. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` ổn định nhưng chỉ có 8.645 hàng, thiếu 455 hàng so với kỳ vọng 9.100. Các hàng thiếu tập trung ở ngày cũ. |
| **P99 độ trễ đo được** | **2,73 ngày**. Max khoảng **2,94 ngày**; tỷ lệ bản ghi tới muộn hơn 1 ngày khoảng **16,5%**. |
| **Lookback đã chọn** | **3 ngày**, lấy theo P99 và làm tròn lên để bao phủ phần lớn dữ liệu tới muộn mà không quét lại lịch sử quá rộng ở mọi lượt chạy. |
| **Nguyên nhân** | Điều kiện `event_date > max(event_date)` chỉ lấy ngày mới hơn ngày lớn nhất đã có. Một event xảy ra ở ngày cũ nhưng tới warehouse muộn sẽ luôn nhỏ hơn `max(event_date)` và bị bỏ lỡ vĩnh viễn. Khi mở rộng lookback, nếu vẫn append thì cùng grain `(event_date, customer_id)` lại bị cộng dồn. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: dùng lookback 3 ngày, `event_date >= max(event_date) - interval '3' day`; thêm `unique_key=['event_date','customer_id']` và `incremental_strategy='merge'`. |
| **Bằng chứng** | Trước: 8.645 hàng. Sau: 9.100 hàng, checksum ổn định; `gold_training_set` vẫn giữ 12.480 hàng. |

**Vì sao dùng P99 thay vì `max`?** P99 là ngưỡng định lượng đại diện cho phần đuôi dữ liệu nhưng ít bị chi phối bởi outlier. Mỗi ngày lookback tăng thêm làm tăng lượng dữ liệu phải quét lại ở mọi lần chạy sau; chọn theo `max` có thể khiến chi phí tăng mạnh chỉ vì một trường hợp cực đoan hiếm gặp.

---

## 3 · Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Từ 08-10 nguồn chuyển `priority` từ số sang nhãn chữ. Pipeline không dừng nhưng Silver có 6.606 hàng NULL/ngoài miền 1..4, trong khi `quarantine_tickets` = 0. |
| **Nguyên nhân** | Macro cũ dùng `try_cast(priority_raw as integer)`: nhãn chữ hợp lệ (`urgent/high/medium/low`) bị biến thành NULL, nhưng các số sai miền như `0`, `5`, `-1` lại được chấp nhận. Contract đang tắt và không có test miền giá trị. |
| **Ba nhóm giá trị** | Nhóm 1: `1..4` → giữ nguyên. Nhóm 2: `urgent/high/medium/low` → map về `1/2/3/4` vì chỉ là schema evolution. Nhóm 3: `P1`, `unknown`, `0`, `5`, `-1`, chuỗi rỗng, NULL → quarantine. |
| **Cách khắc phục** | `normalize_priority.sql`: CASE chuẩn hoá 3 nhóm. `silver_tickets.sql`: lọc bản ghi priority hỏng **trước** `row_number`, nhưng giữ `op='d'` đến sau khi rank để không làm sống lại ticket đã xoá. `quarantine_tickets.sql`: lấy đúng các row macro trả NULL. `schema.yml`: bật `contract.enforced: true`, thêm `not_null` và `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng; `silver_tickets` vẫn đủ 12.480 ticket; priority sạch, luôn 1..4; `dbt test` tăng từ 9 lên 11 test và pass. |

**Thiết kế tầng xử lý:** Bronze nên giữ nguyên dữ liệu thô để phục vụ truy nguyên. Contract/normalization nên áp dụng từ Silver. Khi chỉ có một tập nhỏ bản ghi hỏng, tách chúng vào quarantine cho phép phần dữ liệu tốt tiếp tục phục vụ downstream thay vì dừng toàn bộ pipeline.

---

## 4 · Tổng kết

| Nhiệm vụ | Điều cần kiểm tra đầu tiên khi nhận một pipeline lạ |
|---|---|
| 1 | Grain, natural key và chiến lược ghi của mọi incremental model. |
| 2 | Phân bố `_ingested_at - event_time` và điều kiện lọc incremental có lookback hay không. |
| 3 | Phân bố giá trị ở Bronze/Silver, data contract, test miền giá trị và luồng quarantine. |
