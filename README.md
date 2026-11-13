<p align="center">
  <img src="logo.png" width="120" alt="Trường ĐH Bách Khoa - ĐHQG TP.HCM Logo" />
</p>

# Assignment 1: Distributed Streaming-based Collaboration Application
> **Trường Đại Học Bách Khoa – ĐHQG TP.HCM (HCMUT)**  
> **Khoa Khoa học & Kỹ thuật Máy tính (CSE)**  
> **Môn học:** CO3093 – Mạng Máy Tính / Network Application Development (Học kỳ: HK261)  
> **Giảng viên hướng dẫn:** ThS. Bùi Xuân Giang  
> **Nhóm thực hiện:** Nhóm 02 – Lớp L01  
> • Ngô Tuấn Khôi – MSSV: 2411697  
> • Đặng Phúc Khang – MSSV: 2411421  
> • Huỳnh Tiến Dũng – MSSV: 2410587  

---

## 1. Giới thiệu & Kiến trúc Hệ thống

Ứng dụng cộng tác Client-Server và Peer-to-Peer thời gian thực được xây dựng **100% bằng Socket thuần mức thấp (`import socket`)**, tuân thủ nghiêm ngặt yêu cầu không sử dụng bất kỳ framework mạng bậc cao nào (như Flask, Django, FastAPI) hay WebSockets.

### Sơ đồ Kiến trúc Tổng thể (Dual-Plane Hybrid Architecture)

```text
                     ┌──────────────────────────────────────────────┐
                     │          Central Server (Port 8888)          │
                     │ Auth · Global Chat Hub · E2EE Relay · Watchdog│
                     └──────────────────────┬───────────────────────┘
                                ▲           │           ▲
                TCP Signaling   │           │           │   TCP Signaling
                & Global Chat   │           │           │   & Global Chat
                                ▼           │           ▼
            ┌────────────────────────┐       │       ┌────────────────────────┐
            │      Client: Alice     │       │       │       Client: Bob      │
            │    TCP / UDP Sockets   │       │       │    TCP / UDP Sockets   │
            │  AES-GCM · Web: 5001   │       │       │  AES-GCM · Web: 5002   │
            └───────────┬────────────┘       │       └────────────┬───────────┘
                        │                    │                    │
           REST Polling │         P2P UDP Data Stream             │ REST Polling
                        │  ◄───────────────────────────────────►  │
                        │     (Direct Streaming / AES-256-GCM)   │
                        ▼                                         ▼
            ┌────────────────────────┐               ┌────────────────────────┐
            │     Browser: Alice     │               │      Browser: Bob      │
            │  http://127.0.0.1:5001 │               │  http://127.0.0.1:5002 │
            └────────────────────────┘               └────────────────────────┘
```

Hệ thống phân tách thành hai mặt phẳng độc lập (lấy cảm hứng từ Google QUIC và WebRTC):
1. **Mặt phẳng Báo hiệu (TCP Signaling Plane – Cổng 8888):**
   - Bắt tay 2 chiều Client-Server (`HANDSHAKE_REQ` & `HANDSHAKE_ACK`) (Step 2).
   - Xác thực & Đăng ký an toàn với mật khẩu băm **`scrypt + salt`** (RFC 7914) và cấp phát **Auth Token** (Step 3).
   - Chuyển tiếp tin nhắn tức thời: Broadcast công khai và Unicast bảo mật (Step 5).
   - Duy trì nhịp tim (Heartbeat) và tự thu hồi tài nguyên qua tiến trình **Watchdog** (quét timeout 15s).
2. **Mặt phẳng Dữ liệu (P2P UDP Streaming Plane – Cổng 8889 / Cổng động):**
   - Truyền file tin cậy độc lập với giải thuật **Selective Repeat ARQ** và trượt cửa sổ $N = 8$ (Step 6).
   - Bắt tay đồng bộ `FILE_ANNOUNCE` & `FILE_ANNOUNCE_ACK` trước khi bắt đầu bơm UDP chunk.
   - Phân mảnh tệp $\le 1200\text{ bytes}$ (dưới MTU 1500) giúp loại bỏ hoàn toàn **Head-of-Line (HoL) Blocking**, không làm gián đoạn luồng chat TCP.
   - Đóng hoặc hủy luồng truyền an toàn bằng cờ **`TYPE_FIN`**.
   - Giữ nhịp tim và đồng bộ danh bạ (Peer Discovery) qua UDP có xác thực **Token** (cổng 8889).
3. **Bảo mật Tầng Ứng dụng:**
   - Mã hóa xác thực **AES-256-GCM AEAD** (khóa 256-bit, nonce 12-byte ngẫu nhiên) bảo vệ toàn vẹn payload chat và dữ liệu streaming (Step 8).
4. **Giao diện Web UI Socket thuần (Zero-Framework HTTP/1.1):**
   - Tự xây dựng Server HTTP phân tích chuỗi byte `HTTP/1.1`, phục vụ Dashboard quản lý, Cookies và thanh tiến trình $0\% \to 100\%$ (Step 9).

---

## 2. Cài đặt Môi trường & Thư viện

- **Hệ điều hành:** Windows / macOS / Linux.
- **Phiên bản Python:** Python 3.10 trở lên.
- **Cài đặt thư viện mật mã tiêu chuẩn:**
  ```bash
  pip install -r requirements.txt
  ```
  *(Chỉ bao gồm gói `cryptography` phục vụ thuật toán chuẩn AES-256-GCM; toàn bộ kết nối mạng đều dùng thư viện chuẩn `socket` của Python).*

---

## 3. Hướng dẫn Khởi chạy Hệ thống

### Cách 1: Chạy Demo Tự Động (Khuyên dùng – 1 lệnh duy nhất)
Tự động khởi động Central Server và 2 Client (Alice cổng 5001 & Bob cổng 5002) đồng thời bật sẵn 2 tab trình duyệt:
```bash
python run_demo.py
```
*(Để tắt toàn bộ hệ thống, nhấn `Ctrl + C` tại cửa sổ Terminal).*

---

### Cách 2: Chạy Thủ Công trên từng Terminal (Localhost)

Mở 3 cửa sổ Terminal riêng biệt:

1. **Terminal 1 – Khởi động Server trung tâm:**
   ```bash
   python run_server.py
   ```
   *(Server lắng nghe tại `0.0.0.0:8888` cho TCP và `8889` cho UDP).*

2. **Terminal 2 – Khởi động Client Alice:**
   ```bash
   python run_client.py Alice --ui-port 5001
   ```
   *(Mở trình duyệt Web Dashboard tại `http://127.0.0.1:5001`).*

3. **Terminal 3 – Khởi động Client Bob:**
   ```bash
   python run_client.py Bob --ui-port 5002
   ```
   *(Mở trình duyệt Web Dashboard tại `http://127.0.0.1:5002`).*

*(Bạn có thể mở thêm nhiều client khác như Charlie, David... bằng lệnh `python run_client.py <Tên> --ui-port <Cổng>` tương tự).*

---

### Cách 3: Chạy trên 2 Máy tính Khác Nhau (Cùng mạng LAN / Wi-Fi)

1. **Trên Máy 1 (Server & Alice):**
   - Mở Terminal gõ `ipconfig` (Windows) hoặc `ifconfig` (Linux/macOS) để xem địa chỉ IPv4 LAN (ví dụ: `192.168.1.15`).
   - Khởi động Server:
     ```bash
     python run_server.py --host 0.0.0.0
     ```
   - Khởi động Client Alice:
     ```bash
     python run_client.py Alice
     ```

2. **Trên Máy 2 (Client Bob kết nối tới Máy 1):**
   - Khởi động Bob trỏ về IP của Máy 1:
     ```bash
     python run_client.py Bob --host 192.168.1.15
     ```
   *(Trình duyệt trên Máy 2 tự động mở và kết nối trực tiếp đến Server tại `192.168.1.15:8888`).*

---

## 4. Hướng dẫn Sử dụng Giao diện Web Dashboard

- **Đăng ký & Đăng nhập:**
  - Nhấp nút **Connect** ở góc trên bên phải.
  - Nhập Username và Password: Bấm **Register** để tạo tài khoản mới (mật khẩu băm bằng `scrypt + salt`), hoặc bấm **Login** để đăng nhập.
- **Chat công khai (Global Chat 🌐):** Nhấp chọn kênh *Global Chat* ở cột bên trái để gửi tin nhắn thông báo đến toàn bộ người dùng online.
- **Chat bí mật trực tiếp (Direct Messages 💬):** Nhấp chọn một người dùng cụ thể (như Bob) để gửi tin nhắn E2EE được mã hóa độc quyền giữa hai người.
- **Hệ thống thông báo tin nhắn chưa đọc:** 
  - Tiêu đề tab trình duyệt tự động cập nhật số tin chưa xem: `(3) Assignment 1...`
  - Huy hiệu màu đỏ hiển thị số lượng tin nhắn mới bên cạnh mỗi liên hệ.
- **Truyền tệp Streaming UDP (📁):** 
  - Tại bảng bên phải, chọn người nhận trong menu *Recipient Peer*.
  - Nhấp *Select File* để chọn tệp bất kỳ từ máy tính (ảnh, tài liệu, file nhị phân).
  - Bấm *Start UDP Streaming Transfer*: thanh tiến trình sẽ chạy mượt mà từ $0\%$ đến $100\%$.
  - Người nhận sẽ thấy nút **Open** để mở tệp ngay lập tức (tệp nhận được lưu an toàn tại thư mục `client/downloads/`).

---

## 5. Kiểm thử Tự Động (Automated Test Suite)

Dự án tích hợp bộ 18 bài kiểm thử đơn vị và tích hợp toàn diện:
- Kiểm tra tính đúng đắn của Header nhị phân TCP 20B và UDP 24B, cờ hiệu, CRC32 checksum.
- Mô phỏng kênh truyền rớt gói ngẫu nhiên **20%** kiểm chứng giải thuật **Selective Repeat ARQ** và đóng/hủy luồng qua cờ `TYPE_FIN`.
- Kiểm tra mã hóa xác thực AES-256-GCM AEAD, băm mật khẩu `scrypt + salt`, và token xác thực qua UDP.
- Kiểm tra khả năng mở rộng đồng thời với **15 clients** song song và phòng chống tấn công Path Traversal.

Chạy toàn bộ bài test:
```bash
python run_tests.py
```
Hoặc:
```bash
python -m unittest discover tests -v
```

---

## 6. Cấu trúc Thư mục Dự án

```text
assignment1/
├── docs/                      # Tài liệu thuyết minh đồ án
│   ├── Assignment 1 - Distributed Streaming-based Client-Server Collaboration Application.pdf
│   ├── Report_BTL1.pdf        # Báo cáo kỹ thuật chi tiết BTL1 (14 trang)
│   └── Slide_Assignment1.pdf  # Slide thuyết trình đồ án (41 slide)
├── protocol/                  # Tầng giao thức & Thuật toán truyền thông
│   ├── __init__.py
│   ├── packet.py              # Đóng khung TCP nhị phân 20-byte, mã thông điệp, CRC32
│   ├── crypto.py              # AES-256-GCM AEAD, băm mật khẩu scrypt + salt (RFC 7914)
│   └── udp_stream.py          # Selective Repeat ARQ (Window N=8), TYPE_FIN, UDP Heartbeat
├── server/                    # Tầng Server trung tâm (Signaling Plane)
│   ├── __init__.py
│   ├── server.py              # Server đa luồng TCP/UDP, Watchdog quét timeout
│   ├── user_manager.py        # Quản lý phiên, danh bạ online, lưu trữ tài khoản, token
│   └── run_server.py          # Module thực thi Server
├── client/                    # Tầng Client ứng dụng
│   ├── __init__.py
│   ├── client.py              # Client Network Engine (Dual-plane TCP/UDP)
│   ├── web_server.py          # Server HTTP/1.1 socket thuần (Zero-Framework Web UI)
│   ├── run_client.py          # Module thực thi Client
│   ├── downloads/             # Thư mục lưu tệp nhận qua P2P UDP
│   └── web/                   # Giao diện Web (index.html, style.css, app.js)
├── tests/                     # Bộ kiểm thử tự động
│   ├── __init__.py
│   ├── test_protocol.py       # Kiểm thử nhị phân framing, CRC32, AES-GCM, scrypt, UDP token
│   ├── test_udp_stream.py     # Kiểm thử Selective Repeat với 20% rớt gói, cửa sổ N=8, gói FIN
│   └── test_integration.py    # Kiểm thử tải 15 client, ngắt kết nối đột ngột, REST API
├── requirements.txt           # Danh mục thư viện phụ thuộc
├── run_demo.py                # Script chạy Demo tự động 1 lệnh (Server + 2 Client + 2 Browser)
├── run_server.py              # Phím tắt khởi chạy Server từ thư mục gốc
├── run_client.py              # Phím tắt khởi chạy Client từ thư mục gốc
├── run_tests.py               # Phím tắt thực thi bộ kiểm thử
├── .gitignore                 # Cấu hình bỏ qua tệp tạm, cache và tệp tải về
└── README.md                  # Hướng dẫn chi tiết kỹ thuật và vận hành
```
