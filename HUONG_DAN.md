# Hướng dẫn học và chạy Reliable Transport

## Mục tiêu

Gửi toàn bộ byte từ stdin của sender sang stdout của receiver qua UDP.
Hai bản giữ nguyên giao diện chạy và `utils.py` của đề; không dùng TCP.

## Đọc code theo thứ tự

1. `RTP-base/utils.py`: header 16 byte gồm type, seq_num, length, checksum.
   CRC32 được tính trên cả header (checksum đặt bằng 0) và payload.
2. `RTP-base/sender.py`: `make_packet()` đóng gói; `receive_ack()` kiểm tra ACK;
   `sender()` quản lý START, cửa sổ DATA, timeout và END.
3. `RTP-base/receiver.py`: kiểm tra gói, lưu đệm và xuất byte đúng thứ tự.
4. So sánh với `RTP-opt`: cách ghi nhận ACK và chọn gói cần gửi lại thay đổi.

## Ba biến quan trọng ở sender

- `base`: số thứ tự gói đầu tiên chưa được xác nhận liên tục.
- `next_seq`: số thứ tự gói mới tiếp theo sẽ gửi.
- `deadline`: thời điểm hết hạn timer, đo bằng `time.monotonic()`.

Các gói đã gửi trong cửa sổ nằm trong `[base, next_seq)`.
Chỉ gửi gói mới khi `next_seq < base + window_size`.
Mỗi payload tối đa 1456 byte để cả gói không vượt 1472 byte.

Base dùng ACK cộng dồn: ACK 4 xác nhận các DATA 1, 2, 3.
Nếu ACK hợp lệ lớn hơn base, cập nhật base và đặt lại timer 500 ms.
ACK trùng/cũ không đặt lại timer: nếu đặt lại, một luồng ACK trùng có thể
khiến sender chờ mãi mà không gửi lại gói còn thiếu.
Khi timeout, base gửi lại tất cả gói trong `[base, next_seq)`.

Opt dùng tập `acknowledged` để đánh dấu từng gói.
ACK 3 chỉ xác nhận DATA 3. Cửa sổ chỉ dịch khi gói tại base đã có ACK,
rồi bỏ qua các gói liên tiếp đã được ACK. Timeout chỉ gửi lại những gói
chưa nằm trong tập xác nhận. Cả cửa sổ dùng một timer.

## Hai biến quan trọng ở receiver

- `expected`: số thứ tự DATA tiếp theo cần để nối dữ liệu liên tục.
- `buffered`: dictionary ánh xạ số thứ tự sang payload đến sai thứ tự.

Ví dụ expected=1: nhận DATA 2 thì lưu đệm; nhận DATA 1 thì ghi DATA 1,
ghi tiếp DATA 2 đã lưu và chuyển expected thành 3.
Base trả ACK 3. Opt trả ACK 1 cho lần nhận DATA 1 đó.
Gói đã xuất trước đây vẫn được ACK lại nhưng không ghi lại dữ liệu.
Gói có seq >= expected + window_size bị bỏ.

Receiver kiểm tra kích thước, length và CRC trước khi xử lý trạng thái.
Gói lỗi bị bỏ và không được ACK. Nó chỉ nhận một peer trong một phiên.
START mới trong phiên đang hoạt động bị bỏ qua.

Đầu ra dùng `sys.stdout.buffer.write()`, không dùng `print(bytes)`:
file nhị phân phải giữ đúng mọi byte, không thêm `b'...'` hoặc newline.

## Chạy trên máy Windows hiện tại

Đã tạo `.venv` riêng với Python 3.14 và Scapy 2.8.0.
Scapy 2.4.0 của đề không import được trong môi trường Python 3.14 này.
Nếu cần tạo lại môi trường local:

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements-local.txt
```

Mở hai terminal ở thư mục project. Terminal 1 chạy receiver trước:

```powershell
cmd /c ".venv\Scripts\python.exe RTP-base\receiver.py 127.0.0.1 40000 3 > RTP-base\output.txt"
```

Terminal 2 chạy sender:

```powershell
cmd /c ".venv\Scripts\python.exe RTP-base\sender.py 127.0.0.1 40000 3 < test_scripts\test_message.txt"
```

Dùng cmd để chuyển hướng stdin/stdout dạng byte, tránh khác biệt giữa các
phiên bản PowerShell. So sánh hai file:

```powershell
cmd /c "fc /b RTP-base\output.txt test_scripts\test_message.txt"
```

Thay RTP-base thành RTP-opt để chạy bản tối ưu.

Chạy bộ kiểm tra local:

```powershell
.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Chạy riêng một nhóm để xem từng cơ chế:

```powershell
.venv\Scripts\python.exe -m unittest discover -s tests -k receiver -v
.venv\Scripts\python.exe -m unittest discover -s tests -k timer -v
.venv\Scripts\python.exe -m unittest discover -s tests -k network_faults -v
```

Test `test_opt_duplicate_start_ack_known_spec_ambiguity` được đánh dấu
`expectedFailure`: đó là lỗi đã biết của tình huống START ACK bị nhân bản,
không phải một bài test đã đạt. Kết quả `OK (expected failures=1)` nghĩa
là các bài test khác đạt và tình huống này vẫn chưa được giải quyết.

Các bài kiểm tra dùng UDP thật và subprocess, không mock thuật toán.
Chúng bao gồm dữ liệu rỗng/nhị phân/file mẫu, cửa sổ 1/3/128,
checksum, gói sai thứ tự/trùng/vượt cửa sổ, ACK hỏng/ngoài phạm vi,
gửi lại chọn lọc, START bị mất và END ACK bị mất. Có thêm kiểm tra payload
1456/1457/2912 byte, header length sai, gói quá lớn, sai peer, END đến sớm,
timer đặt lại khi tiến triển và không đặt lại bởi ACK trùng/cũ.
Proxy local trong test cũng tạo mất gói, gói hỏng, trễ, đảo thứ tự và
nhân bản DATA/ACK trong cùng một phiên truyền, với cửa sổ 1/3/128,
hai lần mất DATA liên tiếp, hai lần mất ACK và ACK trễ 650 ms.

Script test.sh và proxy.py gốc giữ nguyên, cần chạy trên Linux/VM của lớp:

```bash
./test_scripts/test.sh RTP-base
./test_scripts/test.sh RTP-opt
```

Nếu báo `Permission denied`, cấp quyền chạy trước:

```bash
chmod +x test_scripts/test.sh test_scripts/compare.sh
```

Script gốc có các giới hạn cần lưu ý khi đánh giá kết quả:

- `compare.sh` in FAILURE nhưng vẫn trả exit code 0. Không dùng riêng
  exit code của script để kết luận đã đạt. Sau mỗi bản, kiểm tra trực tiếp:

  ```bash
  cmp RTP-base/output.txt test_scripts/test_message.txt
  cmp RTP-opt/output.txt test_scripts/test_message.txt
  ```

  `cmp` không in gì và trả mã 0 nếu giống nhau; mã 1 nếu khác nhau.
- `test.sh` dùng `$SENDER_ID` thay vì `$SENDER_PID` ở bước dừng process,
  nên sender bị treo có thể không được dừng. Script còn giới hạn 10 giây
  và không kiểm tra exit code của sender/receiver.
- `proxy.py` gốc dùng `socket.MSG_DONTWAIT`, không có trên Windows native.
  Máy hiện tại chỉ có WSL docker-desktop, Docker engine chưa chạy; chưa
  chạy được script gốc trong một Linux VM ở lần kiểm tra này.

Muốn quan sát từng gói trên VM Linux, mở ba terminal cùng thư mục project:

```bash
# Terminal 1: receiver
python3 RTP-base/receiver.py localhost 40000 3 > RTP-base/output.txt
```

```bash
# Terminal 2: proxy, 0=hỏng byte, 1=trễ, 2=đảo thứ tự, 3=mất gói
python3 test_scripts/proxy.py localhost 50000 localhost 40000 0123
```

```bash
# Terminal 3: sender gửi tới proxy
python3 RTP-base/sender.py localhost 50000 3 < test_scripts/test_message.txt
```

Khi sender/receiver kết thúc, so sánh file bằng `cmp`, rồi Ctrl+C để dừng
proxy. Thay RTP-base thành RTP-opt để kiểm tra bản tối ưu. Có thể thay
window 3 thành 1 hoặc 128 ở cả sender và receiver, hoặc chọn từng lỗi
bằng cách thay `0123` thành `0`, `1`, `2`, `3`.

Receiver chờ mà không in gì khi chưa có sender là bình thường. Nếu có lỗi
port đã được sử dụng, dừng đúng receiver/proxy của lần chạy trước bằng
Ctrl+C hoặc chọn port khác và đổi thống nhất ở các lệnh. Nếu báo thiếu
Scapy trên Windows, dùng Python trong `.venv` thay vì Python toàn máy.

Bộ kiểm tra local không thay thế việc kiểm tra trên VM chấm bài.

## Giả định và giới hạn cần hiểu

- Theo README, không phải xử lý START ACK bị mất hoặc trễ. Sender gửi lại
  START sau 500 ms nếu chưa có ACK để khôi phục trường hợp START bị mất.
  Receiver vẫn bỏ START mới khi đang ở trong phiên, đúng yêu cầu đề.
- ACK của END dùng seq bằng seq của END cộng 1 trong hai chương trình này.
  README không quy định rõ số ACK của END. Sender gửi END một lần và
  chờ tối đa 500 ms. Nếu END mất, receiver có thể còn chờ; nếu END ACK
  mất, sender vẫn thoát sau 500 ms theo yêu cầu đề.
- Định dạng đề có điểm mơ hồ: trong opt, START ACK và DATA 1 ACK đều
  là ACK seq=1, length=0. Nếu START ACK bị nhân bản và một bản đến khi
  DATA 1 đang chờ xác nhận, sender không thể phân biệt bằng các trường
  header hiện có và có thể xác nhận nhầm DATA 1. Review đã tái hiện
  tình huống này với window=1. Muốn bảo đảm cả trường hợp đó cần lớp
  làm rõ giao thức (ví dụ phân biệt ACK điều khiển và ACK dữ liệu).
  Code giữ định dạng của đề, không tự thêm trường hay đổi số thứ tự.
- Sender đọc toàn bộ đầu vào và dựng các gói trong bộ nhớ; đây là cách
  đơn giản để học và kiểm tra. Truyền file rất lớn cần đọc theo cửa sổ.
