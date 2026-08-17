# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Trường An  
**Lớp:** E403  
**Ngày:** 2026-08-17

---

## 1. Kết quả sau khi hoàn thành

Sau khi kiểm tra và sửa các lỗi trong pipeline, kết quả cuối cùng như sau:

```text
gold_training_set     12,480 / 12,480   ổn định ✓
gold_feature_daily     9,100 /  9,100   ổn định ✓
gold_doc_chunks       31,200 / 31,200   ổn định ✓
quarantine_tickets       312 /    312   ổn định ✓
dbt test              11/11 pass
DAG                    catchup=False / max_active_runs=1
Tổng kết               4/4 tiêu chí đạt
```

Checksum của các bảng sau khi chạy lại cũng không thay đổi:

```text
gold_training_set     8dd7c98653
gold_feature_daily    3db448685c
gold_doc_chunks       92d8e50131
quarantine_tickets    ebb89036fb
```

Như vậy, sau khi sửa, pipeline có thể chạy lại nhiều lần mà không làm thay đổi kết quả ngoài mong muốn.

---

## 2. Lỗi `gold_training_set` bị tăng dữ liệu sau mỗi lần chạy

Lỗi đầu tiên mình gặp là bảng `gold_training_set` cứ tăng số dòng sau mỗi lần chạy lại. Ban đầu bảng có 38.750 dòng, trong khi kết quả đúng chỉ cần 12.480 dòng. Khi kiểm tra kỹ thì thấy một `ticket_id` có thể xuất hiện nhiều lần.

Nguyên nhân là model đang chạy theo kiểu `incremental`, nhưng chưa khai báo `unique_key` và cũng chưa dùng chiến lược `merge`. Vì vậy mỗi lần retry, dbt tiếp tục append dữ liệu mới vào bảng thay vì cập nhật bản ghi đã có.

Ngoài ra, nguồn CDC có các bản ghi update (`op='u'`), nên cùng một ticket có thể xuất hiện ở nhiều thời điểm khác nhau. Nếu chỉ xử lý theo partition ngày thì vẫn chưa giải quyết đúng vấn đề vì grain thực tế của bảng là theo `ticket_id`.

Mình sửa `dbt/models/gold/gold_training_set.sql` bằng cách thêm:

```text
unique_key = 'ticket_id'
incremental_strategy = 'merge'
```

Ở DAG mình cũng đặt:

```text
catchup=False
max_active_runs=1
```

Mục đích là tránh việc Airflow tự chạy bù quá nhiều ngày hoặc có nhiều lượt pipeline chạy song song.

Sau khi sửa, `gold_training_set` còn đúng 12.480 dòng. Mỗi ticket chỉ còn một bản ghi và checksum không đổi khi chạy lại nhiều lần.

---

## 3. `gold_feature_daily` bị thiếu dữ liệu của các ngày cũ

Vấn đề tiếp theo nằm ở bảng `gold_feature_daily`. Bảng này không bị tăng dữ liệu như lỗi trên, nhưng chỉ có 8.645 dòng thay vì 9.100 dòng, tức là thiếu 455 dòng.

Khi kiểm tra dữ liệu tới muộn, mình đo được:

- P99 độ trễ khoảng **2,73 ngày**
- Độ trễ lớn nhất khoảng **2,94 ngày**
- Khoảng **16,5%** bản ghi tới muộn hơn 1 ngày

Điều kiện incremental ban đầu chỉ lấy những dòng có:

```text
event_date > max(event_date)
```

Cách này có vấn đề với late-arriving data. Ví dụ một event của ngày cũ nhưng đến warehouse trễ thì `event_date` của nó vẫn nhỏ hơn ngày lớn nhất đã có trong bảng. Kết quả là bản ghi đó không bao giờ được lấy vào ở những lần chạy sau.

Mình chọn lookback **3 ngày** vì P99 là 2,73 ngày, làm tròn lên 3 ngày là đủ bao phủ gần như toàn bộ dữ liệu tới muộn trong bộ dữ liệu của bài.

Điều kiện incremental được đổi thành:

```text
event_date >= max(event_date) - interval '3' day
```

Đồng thời mình thêm:

```text
unique_key = ['event_date', 'customer_id']
incremental_strategy = 'merge'
```

Phần `merge` khá quan trọng. Nếu chỉ mở rộng lookback mà vẫn append thì những ngày được đọc lại sẽ tiếp tục bị ghi trùng.

Sau khi sửa, `gold_feature_daily` đạt đủ **9.100 dòng** và checksum vẫn ổn định qua các lần chạy.

Mình dùng P99 thay vì lấy độ trễ lớn nhất vì trong thực tế có thể có một vài bản ghi tới rất muộn. Nếu lấy `max` để quyết định lookback thì pipeline có thể phải quét lại quá nhiều ngày chỉ vì một số ít trường hợp đặc biệt. Với bộ dữ liệu của lab này, 3 ngày là mức hợp lý.

---

## 4. Cột `priority` thay đổi kiểu dữ liệu

Lỗi thứ ba liên quan tới cột `priority`.

Ở những ngày đầu, `priority` được lưu dưới dạng số từ 1 đến 4. Nhưng từ ngày 08-10, nguồn bắt đầu gửi thêm dạng chữ như `urgent`, `high`, `medium`, `low`.

Pipeline cũ dùng:

```text
try_cast(priority_raw as integer)
```

Do đó các giá trị dạng chữ đều trở thành `NULL`. Trong khi đó một số giá trị số sai như `0`, `5`, `-1` lại vẫn cast sang integer được nên không bị phát hiện ngay.

Khi kiểm tra, Silver có tới 6.606 dòng bị NULL hoặc nằm ngoài miền 1..4 nhưng bảng `quarantine_tickets` lúc đó vẫn bằng 0.

Mình chia dữ liệu thành ba nhóm:

- `1, 2, 3, 4`: giữ nguyên
- `urgent, high, medium, low`: map lần lượt về `1, 2, 3, 4`
- Các giá trị như `P1`, `unknown`, `0`, `5`, `-1`, chuỗi rỗng hoặc NULL: đưa vào quarantine

Macro `normalize_priority.sql` được sửa để xử lý các trường hợp trên bằng `CASE`.

Trong `silver_tickets.sql`, các dòng có priority lỗi được tách ra trước bước `row_number`. Riêng bản ghi delete (`op='d'`) vẫn phải giữ đến sau khi rank, nếu bỏ quá sớm thì có thể làm một ticket đã bị xóa xuất hiện trở lại từ phiên bản cũ.

Mình cũng bổ sung kiểm tra trong `schema.yml`:

```text
contract.enforced: true
not_null
accepted_values: [1, 2, 3, 4]
```

Sau khi sửa:

- `quarantine_tickets` có đúng **312 dòng**
- `silver_tickets` vẫn giữ đủ **12.480 ticket**
- `priority` trong Silver chỉ còn các giá trị từ 1 đến 4
- `dbt test` tăng từ 9 test lên 11 test và **11/11 đều pass**

Theo mình, dữ liệu thô ở Bronze nên được giữ nguyên để sau này còn kiểm tra lại nguồn. Việc chuẩn hóa và kiểm tra kiểu dữ liệu phù hợp hơn khi đưa sang Silver. Những dòng lỗi có thể tách sang quarantine để pipeline vẫn xử lý được phần dữ liệu tốt thay vì dừng toàn bộ.

---

## 5. Kết luận

Qua bài lab này, mình thấy có ba điểm cần kiểm tra khá sớm khi làm việc với một data pipeline dùng incremental model.

Thứ nhất là phải xác định đúng grain và khóa của bảng. Nếu một model incremental không có `unique_key` hoặc dùng sai chiến lược ghi thì retry rất dễ sinh dữ liệu trùng.

Thứ hai là cần kiểm tra độ trễ của dữ liệu trước khi đặt điều kiện incremental. Chỉ lấy dữ liệu mới hơn `max(event_date)` sẽ không phù hợp nếu nguồn có late-arriving data.

Cuối cùng là không nên giả định schema của nguồn sẽ luôn giữ nguyên. Với các cột quan trọng như `priority`, nên có bước normalize, data contract và test miền giá trị. Những dòng không hợp lệ có thể đưa sang quarantine để vừa giữ được dữ liệu lỗi để kiểm tra, vừa không ảnh hưởng tới phần dữ liệu hợp lệ.

Sau các thay đổi trên, cả bốn tiêu chí của bài đều đạt và kết quả giữ ổn định khi chạy lại pipeline.
## Bug khi run môi trường chạy $env:PYTHONUTF8="1"