# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Hữu Chương |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602601 |
| GitHub | https://github.com/hchuong04/ai20k-day09-2A202602601 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07304` dùng với MSSV là 202602601 |

## Kết quả cuối

- `dataset_version` của bạn: `16dda81ac87f`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `733 / 152 / 134` (tổng `1019` khung hình; tương ứng `18 / 4 / 4` trip)
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

- **Số liệu từ `report/evidence/leakage.json`** (trên `1019` khung hình sạch thuộc `26` trip):
  - **Chia ngẫu nhiên theo khung hình (per-frame split 70/15/15)**: Có tới **`26/26` trip bị chia cắt** (`per_frame_split_trips_straddling = 26`), và cả **`26/26` trip đều có khung hình nằm đồng thời ở cả `train` lẫn `test`** (`per_frame_split_trips_in_both_train_and_test = 26`).
  - **Chia theo trip (per-trip split — tầng Gold của pipeline)**: Có **`0` trip bị chia cắt** (`per_trip_split_trips_straddling = 0`), mỗi trip chỉ thuộc duy nhất một tập (`18` trip ở `train`, `4` trip ở `val`, `4` trip ở `test`).
- **Vì sao làm điểm kiểm thử (test score) đẹp giả tạo**:
  - Video được trích xuất ở tốc độ `2 fps` (mỗi khung hình cách nhau `0.5 giây`), nên hai khung hình liên tiếp trong cùng một trip có cảnh nền, ánh sáng, thời tiết, vạch kẻ đường và xe phía trước gần như giống hệt nhau.
  - Nếu khung ở giây `t` nằm trong `train` và khung ở giây `t + 0.5s` nằm trong `test`, mô hình chỉ cần ghi nhớ (memorize) đặc trưng cảnh nền của trip đó là đoán trúng nhãn trên tập `test` (data leakage), tạo ra độ chính xác rất cao trên giấy nhưng sẽ thất bại nặng nề khi triển khai ra đường thật gặp cảnh mới.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

- **Vì sao `dataset_version` là băm nội dung (`sha256` của danh sách `frame_id|split`)**:
  - Dùng băm nội dung đảm bảo tính **tất định (deterministic) và bất biến**: dữ liệu đầu vào và quy tắc chia không đổi thì mã phiên bản không bao giờ đổi, dù chạy lại ở bất kỳ máy nào hay vào thời điểm nào. Nếu dùng ngày giờ (`timestamp`) hoặc số đếm tự tăng (`v1, v2`), mỗi lần chạy lại pipeline (backfill/retry) sẽ sinh ra một version mới dù dữ liệu bên trong giống hệt nhau, gây tốn dung lượng lưu trữ và không biết hai mô hình có đang huấn luyện trên cùng một tập dữ liệu thật hay không.
- **Hai lần chạy liên tiếp**: Khi chạy `step 4` rồi chạy tiếp `cli export-dataset`, cả hai lần đều cho ra đúng một `dataset_version` giống hệt nhau là **`16dda81ac87f`** (và `manifest_sha256 = 105bf32a5faac8af64454bc47c3c64eac84a1397063e1f510987d94597a23810`).
- **Vì sao `dataset_version` của tôi khác bạn bè dù cùng dữ liệu gốc**:
  - Trong `gold_dataset_frames.sql`, thứ tự các trip trong mỗi drive được sắp xếp theo biểu thức `order by md5(trip_id || '{{ env_var("LAB_STUDENT_ID", "") }}')` trước khi gán `70%` đầu vào `train` (`9` trip/drive), `15%` tiếp vào `val` (`2` trip/drive) và `15%` cuối vào `test` (`2` trip/drive).
  - Vì `LAB_STUDENT_ID = 2A202602601` của tôi đóng vai trò chuỗi muối (salt) trong hàm băm `md5`, thứ tự xếp hạng các trip của tôi khác với bạn bè $\rightarrow$ tập hợp các trip rơi vào `train/val/test` khác nhau (`733 / 152 / 134` khung hình) $\rightarrow$ chuỗi `frame_id|split` khác nhau $\rightarrow$ mã băm `dataset_version` (`16dda81ac87f`) là duy nhất cho riêng MSSV của tôi.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

- **Dataset `16dda81ac87f` của tôi được tạo từ dữ liệu nào (theo `lineage` và `rules` trong `manifest.json`)**:
  - **Các bảng Delta nguồn và phiên bản commit (`version`) tương ứng**: `bronze.raw_telemetry_a` (version `0`), `bronze.raw_telemetry_b` (version `0`), `bronze.trips_raw` (version `0`), `bronze.video_files` (version `0`), `silver.frames` (version `25` — sau khi nạp 26 partition trip từ `0..25`), `silver.telemetry` (version `0`), `silver.frames_flagged` (version `0`), và `silver.frame_telemetry` (version `0`).
  - **Mã băm SHA256 của 2 file video gốc (`input_video_sha256`)**:
    - `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
    - `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
  - **Quy tắc lọc & chia (`rules`)**: Loại bỏ các khung hình vi phạm `["is_black", "is_blurry", "is_near_dup", "not sync_ok"]` và chia `split` theo trip phân tầng trên từng drive bằng thứ tự `md5`.
- **Thông tin giúp tái tạo lại đúng dataset này sau 3 tháng**:
  - Nhờ cơ chế **Time Travel của Delta Lake**, ta chỉ cần truy vấn lại đúng số `version` của các bảng Bronze/Silver đã ghi trong `lineage`, kiểm tra đúng mã `input_video_sha256` của video đầu vào, áp dụng bộ `rules` cùng muối `LAB_STUDENT_ID = 2A202602601` (và commit Git của code pipeline) là sẽ tái tạo lại chính xác 100% tập dữ liệu `16dda81ac87f` cùng `manifest_sha256` ban đầu, ngay cả khi các bảng trong Lakehouse đã có thêm hàng trăm phiên bản mới.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

- **Khi đặt `LAB_FAIL_ONCE=trip-07304` và chạy DAG `day09_video_pipeline`**:
  - Trong nhóm `silver_frames [26]`, đúng **1 mapped task instance (`map_index=19` ứng với `trip-07304`) chuyển đỏ (`failed`)** với log `RuntimeError: simulated crash while extracting trip-07304; run it again` (xem ảnh `report/screenshots/fail_task_log.png`), trong khi **25/26 task con còn lại vẫn chạy thành công (`success`)**.
  - Toàn bộ **7 task hạ nguồn** phía sau (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`) chuyển sang màu cam **`upstream_failed`** và dừng lại, không cho phép dữ liệu thiếu đi tiếp.
- **Khi phục hồi (Clear task)**:
  - Tôi chỉ bấm **`Clear task`** trên đúng instance bị đỏ (`silver_frames [19]`, kèm các task downstream của nó). Airflow chỉ chạy lại **đúng 1 partition `trip-07304`** (không phải giải mã lại 25 trip đã xanh) rồi chạy tiếp 7 task hạ nguồn cho đến khi cả DAG chuyển sang **xanh toàn bộ** (`report/screenshots/airflow_retry_graph.png` và `report/screenshots/airflow_graph.png`).
  - **Dữ liệu không hề bị nhân đôi** (tổng số khung hình ở Silver vẫn đúng **`1246` dòng**, ở Gold vẫn **`1019` dòng** nhờ cơ chế Delta `overwrite` theo predicate `trip_id`), kiểm tra `verify` vẫn đạt **`27/27`** và **`dataset_version` giữ nguyên không đổi là `16dda81ac87f`**.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Khi tăng quy mô từ 2 chuyến xe (~27 MB video, ~6 nghìn dòng telemetry) lên **500 xe mỗi ngày** (~hàng chục TB video 7 camera và hàng tỷ bản ghi CAN/GPS mỗi ngày), **3 thành phần sẽ vỡ đầu tiên** là:
1. **Giải mã video FFmpeg & điều phối bằng `LocalExecutor` trên một máy đơn (`airflow` container)**: 500 xe $\times$ nhiều trip sẽ tạo ra hàng chục nghìn tác vụ nặng về CPU/GPU làm nghẽn cổ chai và tràn RAM (OOM) một máy chủ. **Thay bằng**: Cụm xử lý phân tán **Ray Data / Spark** trên Kubernetes kết hợp phần cứng tăng tốc giải mã video (NVIDIA NVDEC), điều phối qua **Airflow `KubernetesExecutor` hoặc `CeleryExecutor`** (liên hệ slide 12, 23).
2. **Engine biến đổi dữ liệu `DuckDB` (in-process, single-node) trên file cục bộ `work/lakehouse.duckdb`**: DuckDB chỉ chạy trên một node và bị khóa file khi ghi đồng thời, không thể join ASOF hàng tỷ dòng telemetry với hàng trăm triệu khung hình. **Thay bằng**: Engine tính toán phân tán **Apache Spark (Databricks) hoặc Trino** chạy `dbt-spark` / `dbt-trino` đọc-ghi trực tiếp trên Delta Lake (liên hệ slide 22).
3. **Cách nạp telemetry theo lô file CSV tĩnh và copy từng ảnh JPG sang thư mục Gold trên S3**: Nạp file CSV thủ công gây trễ lớn, còn lệnh `s3.copy_object` cho hàng trăm triệu file ảnh nhỏ vào từng thư mục `gold/datasets/<version>/` sẽ làm nghẽn giới hạn request IOPS của S3/MinIO và tốn chi phí lưu trữ gấp bội. **Thay bằng**: Nạp telemetry qua **Kafka + Spark Structured Streaming / Flink** vào Delta Lake; ở tầng Gold chỉ xuất **manifest Parquet/Lance/WebDataset (chứa con trỏ URI + byte range)** thay vì nhân bản lại các file ảnh vật lý (liên hệ slide 12, 23).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

**1. Câu hỏi vận hành 1 — Top 10 trip có tỷ lệ khung hình bị loại bỏ cao nhất:**

```sql
SELECT
    drive_id,
    trip_id,
    split,
    avg_speed_kmh,
    frames AS total_frames,
    frames_kept,
    (frames - frames_kept) AS frames_dropped,
    ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 1) AS drop_rate_pct
FROM marts.mart_trip_summary
ORDER BY drop_rate_pct DESC
LIMIT 10;
```

- **Điều câu truy vấn cho biết** (xem ảnh `report/screenshots/metabase_dashboard1_sql.png`): Bảng xếp hạng 10 trip có tỷ lệ khung hình bị loại bỏ cao nhất cho thấy các trip thuộc `drive_a` như `trip-04622`, `trip-03904`, `trip-02086`, `trip-08888` (đặc biệt `trip-08888` có tốc độ trung bình rất thấp chỉ `6.5 km/h` và `trip-07774` là `12.8 km/h`) có tỷ lệ khung hình bị loại cao nhất. Nguyên nhân là ở `drive_a` xe di chuyển chậm/dừng chờ và có sự cố đứng hình camera 6 giây cùng các khung mờ/đen, khiến bộ lọc `is_near_dup` (dHash) và chất lượng loại bỏ nhiều khung dư thừa; ngược lại ở `drive_b` xe chạy tốc độ cao (`58.3 - 91.2 km/h`) nên cảnh biến thiên liên tục, tỷ lệ khung hình giữ lại cho huấn luyện rất cao.

**2. Câu hỏi vận hành 2 (Bonus) — Phân tích dải tốc độ xe và cơ cấu loại bỏ khung hình theo chuyến (Drive):**

```sql
SELECT
    f.drive_id,
    f.dropped_black,
    f.dropped_blurry,
    f.dropped_near_dup,
    f.dropped_out_of_sync,
    COUNT(DISTINCT t.trip_id) AS total_trips,
    SUM(t.frames) AS total_frames,
    SUM(t.frames_kept) AS kept_frames,
    ROUND(100.0 * SUM(t.frames_kept) / SUM(t.frames), 1) AS kept_pct,
    ROUND(AVG(t.avg_speed_kmh), 1) AS mean_trip_speed_kmh
FROM marts.mart_frame_funnel f
JOIN marts.mart_trip_summary t USING (drive_id)
GROUP BY f.drive_id, f.dropped_black, f.dropped_blurry, f.dropped_near_dup, f.dropped_out_of_sync
ORDER BY f.drive_id;
```

- **Ý nghĩa vận hành**: Truy vấn kết hợp `mart_frame_funnel` và `mart_trip_summary` cho thấy bức tranh đối lập giữa hai chuyến xe: `drive_a` (vận tốc trung bình thấp, nhiều đoạn nội đô/dừng chờ) chỉ giữ lại được **`436/602` khung hình (`72.4%`)** với nguyên nhân loại bỏ áp đảo là `dropped_near_dup = 149` và `dropped_blurry = 9`; trong khi `drive_b` (chạy đường trường tốc độ cao) giữ lại được **`583/644` khung hình (`90.5%`)** và không có khung mờ nào (`dropped_blurry = 0`). Từ góc độ vận hành đội xe thu thập dữ liệu, cần tự động giảm tần suất lấy mẫu (adaptive FPS) khi tốc độ CAN bus `< 15 km/h` để tiết kiệm băng thông và bộ nhớ lưu trữ ngay từ trên xe.

---

## Bonus (+5 điểm) — Thí nghiệm riêng: Đánh giá tác động của `sync_tol_ms` và `split_train / split_val`

### Thí nghiệm mở rộng A: Thay đổi dung sai đồng bộ thời gian `sync_tol_ms` trong `dbt/dbt_project.yml`
- **Thiết lập**: Trong `dbt/dbt_project.yml`, biến `sync_tol_ms` quy định độ lệch thời gian tối đa (`|frame.event_time - telemetry.ts|`) được phép khi join ASOF giữa khung hình video (`2 fps`) và dòng telemetry sạch (`~10 Hz`). Giá trị mặc định là **`150 ms`**. Tôi thử siết chặt xuống **`50 ms`** và nới lỏng lên **`500 ms`**, chạy lại `dbt run --select silver_frame_telemetry gold_dataset_frames` và đo số khung hình bị loại do mất đồng bộ (`not sync_ok`) cùng số khung hình còn lại ở tầng Gold:

| Cấu hình `sync_tol_ms` | Khung mất đồng bộ (`drive_a` / `drive_b`) | Tổng khung Gold giữ lại | Số khung theo `split` (`train / val / test`) | Chênh lệch so với mặc định |
|---|---|---|---|---|
| **Trước khi thử (`150 ms` — mặc định)** | `7` / `7` (tổng `14` khung) | **`1019` khung** | `733 / 152 / 134` (`18 / 4 / 4` trip) | Chuẩn gốc (`dataset_version = 16dda81ac87f`) |
| **Sau khi thử 1 (`50 ms` — siết chặt)** | `271` / `327` (tổng `598` khung) | **`538` khung** | `388 / 72 / 78` (`18 / 4 / 4` trip) | **Mất thêm `481` khung sạch (`-47.2%`)** |
| **Sau khi thử 2 (`500 ms` — nới lỏng)** | `5` / `6` (tổng `11` khung) | **`1021` khung** | `735 / 152 / 134` (`18 / 4 / 4` trip) | Chỉ tăng thêm `+2` khung |

- **Giải thích nguyên nhân chênh lệch**:
  - Cảm biến telemetry lấy mẫu ở tần số khoảng `10 Hz` (chu kỳ ~`100 ms` giữa hai mẫu), nên độ lệch thời gian trung bình tự nhiên giữa một khung hình và mẫu telemetry gần nhất ở `drive_a` là `66.0 ms` và ở `drive_b` là `73.0 ms`.
  - Khi hạ `sync_tol_ms` xuống **`50 ms`** (nhỏ hơn độ trễ trung bình `66–73 ms`), gần một nửa số khung hình hoàn toàn bình thường bị đánh cờ `sync_ok = false` oan và bị loại khỏi tập Gold (giảm từ `1019` xuống chỉ còn `538` khung hình).
  - Ngược lại, khi nới lên **`500 ms`**, số khung Gold chỉ tăng thêm `2` khung (lên `1021`), vì các khung mất đồng bộ còn lại nằm ở đúng các khoảng trống cảm biến bị đứt tín hiệu dài từ `2884 ms` đến `3112 ms` (đã được cảnh báo bởi `max_gap_ms`). Điều này chứng minh ngưỡng mặc định `150 ms` ($\approx 1.5\times$ chu kỳ lấy mẫu cảm biến) là điểm tối ưu: giữ được toàn bộ các khung hình trong điều kiện hoạt động bình thường nhưng vẫn loại bỏ chính xác các khung hình rơi vào vùng mù khi cảm biến mất tín hiệu.

### Thí nghiệm mở rộng B: Thay đổi tỷ lệ phân chia `split_train / split_val` trong `dbt/dbt_project.yml`
- **Thiết lập**: Thay đổi tỷ lệ mặc định `split_train = 0.70, split_val = 0.15` (tức `70% / 15% / 15%`) sang **`split_train = 0.60, split_val = 0.20`** (tức `60% / 20% / 20%`) với cùng `LAB_STUDENT_ID = 2A202602601`:

| Cấu hình tỷ lệ (`train / val / test`) | Số trip mỗi tập (`train / val / test`) | Số khung hình mỗi tập (`train / val / test`) | Tổng khung Gold | Số trip bị rò rỉ (straddling) |
|---|---|---|---|---|
| **Trước khi thử (`0.70 / 0.15 / 0.15`)** | `18 / 4 / 4` (tức `9 / 2 / 2` mỗi drive) | **`733 / 152 / 134`** | `1019` | `0` trip |
| **Sau khi thử (`0.60 / 0.20 / 0.20`)** | `16 / 4 / 6` (tức `8 / 2 / 3` mỗi drive) | **`627 / 175 / 217`** | `1019` | `0` trip |

- **Giải thích tác động**: Vì mỗi drive có `13` trip và công thức chia dùng `round(13 * ratio)`, khi đổi sang `0.60 / 0.20`, mốc `round(13 * 0.60) = round(7.8) = 8` trip/drive được gán vào `train` (tổng `16` trip = `627` khung), mốc `round(13 * 0.80) = round(10.4) = 10` nên `val` vẫn nhận `10 - 8 = 2` trip/drive (nhưng vì trip thứ 9 và 10 của thứ tự `md5` có nhiều khung hình hơn trip thứ 10 và 11 trước đó nên `val` tăng từ `152` lên `175` khung), và `test` nhận `13 - 10 = 3` trip/drive (tổng `6` trip = `217` khung). Dù đổi tỷ lệ, tính chất **không rò rỉ theo cảnh (`0` trip chia cắt)** vẫn được bảo toàn tuyệt đối.
- **Khôi phục cấu hình chuẩn**: Sau khi ghi nhận số liệu, tôi đã đưa `dbt_project.yml` và các bảng về đúng cấu hình gốc (`sync_tol_ms = 150`, `split_train = 0.70`, `split_val = 0.15`), kiểm tra lại `dataset_version` đạt đúng **`16dda81ac87f`** và `verify` đạt **`27/27 checks passed`**.

---

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- **Điều bất ngờ nhất**: Chỉ với một phép băm nội dung đơn giản (`sha256` trên `frame_id|split`) kết hợp `overwrite` theo partition của Delta Lake và Dynamic Task Mapping của Airflow, khi 1 trip bị sập và chạy lại chỉ đúng 1 task đó, toàn bộ pipeline vẫn tái tạo ra chính xác 100% `dataset_version = 16dda81ac87f` mà không bị nhân đôi bất kỳ khung hình nào.
- **Điều còn muốn tìm hiểu thêm**: Trong thực tế khi có 7 camera quanh xe cùng lúc, việc đồng bộ event-time giữa nhiều luồng video lệch chuẩn FPS và LiDAR/RADAR sẽ được thiết kế trong bảng Silver như thế nào để tối ưu chi phí join.
- **Một lỗi dữ liệu mà pipeline hiện tại chưa bắt được**: Lỗi **nhãn cảm biến bị kẹt giá trị hợp lệ (stuck sensor / frozen telemetry)** hoặc **lệch lắp đặt camera (camera bị nghiêng/lệch góc sau va chạm hoặc dính bùn một góc)**. Ví dụ tốc độ `speed_mps` bị treo cứng ở `15.0 m/s` suốt 30 giây: giá trị này vẫn nằm trong khoảng `[0, 70]`, không `NULL`, timestamp vẫn tăng đều nên vượt qua toàn bộ các bài test `in_range`, `not_null`, `max_gap_ms` hiện tại dù xe thực tế đã dừng lại.
