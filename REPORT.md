# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Hữu Chương |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602601 |
| GitHub | https://github.com/hchuong04/ai20k-day09-2A202602601 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

4 khác biệt chính giữa `raw_telemetry_a` (3021 dòng) và `raw_telemetry_b` (3234 dòng) ở tầng Bronze:
1. **Tên cột khác nhau**: Nguồn A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`; nguồn B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`.
2. **Kiểu thời gian và đồng hồ**: Nguồn A lưu chuỗi ISO 8601 UTC (`2025-10-07T08:00:00.014Z`); nguồn B lưu số nguyên Unix epoch milliseconds (`1759933801509`) và đồng hồ thiết bị B bị lệch `+1500 ms` so với thời gian thực của video.
3. **Đơn vị tốc độ**: Nguồn A đo bằng `m/s` (`speed_mps = 1.213`); nguồn B đo bằng `km/h` (`spd_kmh = 55.11`).
4. **Thang đo bàn đạp ga/phanh**: Nguồn A dùng tỷ lệ thập phân `0..1` (`gas = 0.028`); nguồn B dùng phần trăm `0..100%` (`throttle_pct = 2.0`).

Pipeline giữ **mỗi nguồn một bảng Bronze riêng** và lưu **mọi cột ở dạng chữ (`VARCHAR`)** vì tầng Bronze có nhiệm vụ lưu trữ nguyên bản (raw, append-only) đúng như dữ liệu từ cảm biến gửi về. Nếu gộp và ép kiểu ngay lúc ingest, các dòng có ô trống, lỗi định dạng hoặc thay đổi cấu trúc (schema drift) sẽ làm sập quá trình nạp hoặc làm mất dữ liệu gốc, khiến hệ thống không thể truy vết lỗi hay chạy lại (reprocess) khi cần cập nhật logic xử lý ở tầng Silver.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

- **Cách chuẩn hoá về một schema thống nhất** (qua `stg_telemetry_a.sql` và `stg_telemetry_b.sql`):
  - **Tên cột & kiểu dữ liệu**: Đổi toàn bộ tên cột của nguồn B (`spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`) về cùng tên cột với nguồn A và ép kiểu số thực (`DOUBLE`), dùng `try_cast(nullif(..., '') as double)` để ô trống thành `NULL` mà không gãy truy vấn.
  - **Đơn vị**: Tốc độ nguồn B chia `3.6` (`spd_kmh / 3.6 -> speed_mps`); bàn đạp ga/phanh nguồn B chia `100` (`throttle_pct / 100 -> gas`, `brake_pct / 100 -> brake`) để đưa về thang `0..1`.
  - **Kiểu thời gian & độ lệch đồng hồ**: Nguồn A parse chuỗi ISO sang UTC timestamp; nguồn B lấy `timestamp_ms` trừ đi `telemetry_clock_offset_ms` (`1500 ms` từ `bronze.video_files`) rồi chia `1000.0` qua `to_timestamp((cast(timestamp_ms as bigint) - offset_ms) / 1000.0)` để về cùng đồng hồ UTC với video.
- **Bằng chứng cho thấy đã đúng**: Sau chuẩn hoá, tốc độ tối đa của cả 2 drive đều ở mức hợp lý (`drive_a`: `71.0 km/h`, `drive_b`: `100.0 km/h`, đều `< 130 km/h`). Thời điểm mẫu đầu tiên (`bat_dau`) của `drive_b` là `2025-10-08 14:30:00.009000+00:00` (thay vì `14:30:01.509`), khớp sát với mốc bắt đầu video `14:30:00` (chênh lệch chỉ `9 ms < 0.5 s`). Tổng số dòng bảo toàn tuyệt đối: `drive_a` có `2968 sạch + 53 lỗi = 3021`; `drive_b` có `3181 sạch + 53 lỗi = 3234`.
- **Bảng quarantine (`silver.silver_telemetry_rejects` — tổng 106 dòng)**:

| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

- **Vì sao đưa dòng lỗi vào `rejects` thay vì xoá**: Giúp không mất dấu dữ liệu (đối chiếu tổng số dòng `clean + quarantined == received`), cho phép theo dõi chất lượng cảm biến theo từng nguyên nhân lỗi để cảnh báo phần cứng, và có thể khôi phục/chạy lại nếu phát hiện ngưỡng lọc ban đầu đặt sai.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

- **Phân biệt `warn` và `error`**:
  - `max_gap_ms` đặt mức `warn` vì việc cảm biến GPS/CAN bus bị rớt nhịp vài giây (do đi vào hầm, nhiễu sóng) là hiện tượng vật lý thực tế. Các mẫu còn lại vẫn đúng nên chỉ cần cảnh báo để kỹ sư nắm được chất lượng thu thập, không được chặn toàn bộ pipeline (các khung hình ở khoảng trống đó sẽ được xử lý riêng bằng cờ `sync_ok` khi join).
  - `in_range` (kiểm tra `speed_mps` trong đoạn `[0, 70]`) đặt mức `error` vì tốc độ âm hoặc `> 70 m/s` (`> 252 km/h`) là dữ liệu sai vật lý nghiêm trọng. Nếu để lọt xuống tầng Gold sẽ làm hỏng nhãn huấn luyện mô hình xe tự lái.
- **Kết quả thí nghiệm `break-gate` (`report/evidence/break_gate.json`)**:
  - Khi nới ngưỡng lọc `max_speed_mps` từ `70` lên `1000 m/s`, các dòng tốc độ vô lý lọt qua bước lọc vào bảng `silver_telemetry`.
  - Khi chạy `dbt test`, kết quả trả về `PASS=5 WARN=1 ERROR=1 SKIP=0` (`gate_failed: true`): bài test **`in_range_silver_telemetry_speed_mps__70__0`** lập tức báo **FAIL/ERROR** và bắt được chính xác **6 dòng** vi phạm (3 dòng của `drive_a` và 3 dòng của `drive_b`). Sau khi khôi phục luật gốc, kết quả trở lại `PASS=6 WARN=1 ERROR=0 SKIP=0`.
  - **Vai trò của cổng chất lượng (Quality Gate)**: Đóng vai trò chốt chặn độc lập (circuit breaker) ngay sau bước biến đổi; nếu logic code/SQL vô tình bị sửa sai hoặc nới lỏng làm lọt dữ liệu bẩn, cổng chất lượng sẽ dừng DAG ngay lập tức để ngăn dữ liệu rác lan truyền xuống tầng Gold, Warehouse và tập huấn luyện ML.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

- **Số liệu từ `report/evidence/idempotency.json` (chạy lại trên `trip-00003` gồm `60` khung hình)**:
  - **Cách của pipeline (Delta `overwrite` theo predicate `trip_id = 'trip-00003'`)**: Trước khi chạy lại bảng có **`1246` dòng** (dấu vân tay `6c7480dfde7e`); sau **2 lần chạy lại** bảng vẫn giữ nguyên **`1246` dòng** và dấu vân tay **`6c7480dfde7e`** (`unchanged: true`).
  - **Cách ghi sai (`append`)**: Chỉ riêng `trip-00003` (vốn có `60` khung hình) sau 2 lần chạy đã phình lên thành **`120` dòng** (nhân đôi $2\times$), dẫn đến trùng lặp dữ liệu huấn luyện.
- **Vì sao Delta `overwrite` theo partition an toàn hơn "xoá rồi ghi lại" (DELETE + INSERT)**:
  - Delta Lake thực hiện `overwrite` với `predicate` ở dạng **giao dịch nguyên tử (ACID transaction)** thông qua `_delta_log`: dữ liệu mới được ghi ra các file Parquet mới trước, rồi mới ghi một commit duy nhất vừa đánh dấu gỡ (remove) file cũ của partition đó vừa thêm (add) file mới.
  - Nếu làm thủ công kiểu "xoá rồi ghi lại" mà tiến trình bị sập (crash, mất điện, đầy ổ đĩa) ngay sau bước xoá và trước khi ghi xong, partition đó sẽ bị **mất trắng hoặc chỉ còn một nửa dữ liệu**, trong khi các reader đang đọc bảng tại thời điểm đó cũng thấy dữ liệu bị thiếu. Với Delta `overwrite`, nếu sập giữa chừng thì commit không được tạo và bảng vẫn giữ nguyên trạng thái đúng trước đó.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

- **Khung hình đen (`is_black`) vs Khung hình tối (`is_dark`)**:
  - **Khung đen** (`brightness < 12.0`): Là ảnh gần như tối đen hoàn toàn do lỗi phần cứng camera, che khuất ống kính hoặc rơi gói dữ liệu video (`drive_a` có `6` khung, `drive_b` có `4` khung). Pipeline **loại bỏ** không đưa vào tập Gold.
  - **Khung tối** (`12.0 <= brightness < 45.0`): Là cảnh thực tế khi xe đi qua đường hầm, dưới gầm cầu hoặc bóng râm dày (`drive_a` có `4` khung, `drive_b` có `61` khung). Pipeline **gắn cờ `is_dark` nhưng vẫn giữ lại** (`toi_giu_lai`). Tôi **hoàn toàn đồng ý** với cách xử lý này: nếu loại bỏ khung tối thì mô hình xe tự lái sẽ bị thiên lệch (bias), chỉ biết lái khi trời nắng đẹp và dễ gây tai nạn khi đi vào đường hầm hoặc lúc chiều tối.
- **Lựa chọn ngưỡng dHash và mối liên hệ với tốc độ xe**:
  - Tôi chọn **ngưỡng `2` bit** (`threshold_in_use = 2`, loại `149/602` khung ở `drive_a` và `53/644` khung ở `drive_b`). Ngưỡng `0` quá khắt khe (chỉ bắt các khung giống hệt từng bit, bỏ sót cả những khung camera bị đứng hình nhưng có nhiễu nén video MP4 nhẹ); ngược lại ngưỡng `6` quá lỏng (loại tới `363/602` khung ở `drive_a` — hơn 60% dữ liệu, làm mất cả những khung hình xe đang di chuyển thật).
  - **Phụ thuộc vào tốc độ xe**: Khi xe dừng đèn đỏ, tắc đường hoặc chạy rất chậm (như ở `drive_a`), cảnh vật giữa 2 khung hình liên tiếp (cách nhau `0.5s` ở `2 fps`) hầu như không thay đổi nên khoảng cách Hamming của dHash rất nhỏ ($\le 2$ bit). Khi xe chạy nhanh (như `drive_b` đạt tới `100 km/h`), cảnh vật dịch chuyển nhiều hơn nên ít khung trùng hơn (`53` khung so với `149` khung ở `drive_a`).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
