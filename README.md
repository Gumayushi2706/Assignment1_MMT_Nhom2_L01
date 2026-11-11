<p align="center">
  <img src="logo.png" width="120" alt="Trường ĐH Bách Khoa - ĐHQG TP.HCM Logo" />
</p>

# Assignment 1: Distributed Streaming Collaboration System (NexusStream)
> **Course:** CO3093 / CO3094 – Mạng Máy Tính & Phát triển Ứng dụng Mạng (Computer Networks & Network Application Development)  
> **Assignment 1:** Distributed and Streaming-based Client-Server Collaboration Application  

---

## 1. Giới thiệu Tổng quan

Hệ thống cộng tác thời gian thực phân tán **NexusStream** được xây dựng **100% bằng socket nhị phân mức thấp** (`import socket` trong Python), hoàn toàn không sử dụng các web framework hay thư viện mạng bậc cao (như Flask, Django, Tornado, Express, WebSockets) theo đúng quy định đề bài:

- **Kiến trúc phân tách 2 kênh (Dual-Plane Architecture):**
  - **TCP Signaling Plane:** Bắt tay 2 chiều TCP, xác thực đăng nhập duy nhất, theo dõi phiên (Session Management), tự động phát hiện người dùng (Peer Discovery), gửi nhận tin nhắn chat thời gian thực (Broadcast & Unicast) và phát tín hiệu báo trước khi truyền file (`FILE_ANNOUNCE`).
  - **UDP Data Streaming Plane:** Truyền dữ liệu file tin cậy độc lập bằng giải thuật **Selective Repeat ARQ** (cửa sổ trượt Sliding Window $N=8$, kích thước chunk 1200 bytes $< \text{MTU } 1500\text{B}$ tránh phân mảnh IP), loại bỏ hoàn toàn hiện tượng nghẽn đầu hàng (**Head-of-Line Blocking**).
- **Bảo mật Tầng Ứng dụng (E2EE):** Mã hóa xác thực **AES-256-GCM** (chuẩn AEAD) với khóa đối xứng 32B và nonce ngẫu nhiên 12B cho cả nội dung tin nhắn và từng chunk dữ liệu UDP (Server chỉ đóng vai trò Zero-Knowledge Relay).
- **Web UI thuần Socket (Zero-Framework):** Tự xây dựng máy chủ HTTP/1.1 socket thuần phục vụ giao diện Web Dashboard (HTML5, Modern CSS, Vanilla JS), quản lý trạng thái bằng Cookie RFC 6265, cập nhật danh bạ và hiển thị thanh tiến trình truyền file thời gian thực (đáp ứng trọn vẹn Step 9).

---

## 2. Bảng Đối chiếu 10 Tiêu chí Đề bài (Rubric 10/10 Điểm)

| Tiêu chí (Milestone) | Yêu cầu theo Rubric | Hiện thực trong Mã nguồn | Điểm |
| :--- | :--- | :--- | :---: |
| **Step 1: Setup & Protocol Design** | Header nhị phân, cờ Flags, CRC32, Framing. | [`protocol/packet.py`](protocol/packet.py): Header TCP 20B cố định `!HBBB3sIII`, hàm `recv_exact` chống dính/xé gói. | 1 đ |
| **Step 2: Socket Setup & Handshake** | Bắt tay 2 chiều TCP giữa Client và Server. | [`server/server.py`](server/server.py) & [`client/client.py`](client/client.py): Cặp gói tin `HANDSHAKE_REQ` và `HANDSHAKE_ACK`. | 1 đ |
| **Step 3: Auth & State Management** | Đăng nhập tên duy nhất, quản lý phiên, lưu IP/Port. | [`server/user_manager.py`](server/user_manager.py): Lưu `UserSession` an toàn đa luồng (`RLock`), broadcast `USER_JOINED`. | 1 đ |
| **Step 4: Concurrency Implementation** | Đa luồng (Multi-threading), hỗ trợ 10–20 clients. | Mô hình Thread-per-Client, kiểm thử thành công 15 clients đồng thời trong [`tests/test_integration.py`](tests/test_integration.py). | 1 đ |
| **Step 5: Time-sensitive Streaming** | Định tuyến chat chung (Broadcast) và riêng (Unicast). | Gói tin `MSG_BROADCAST`, `MSG_UNICAST`, kèm gói xác nhận `MSG_DELIVERY_ACK`. | 1 đ |
| **Step 6: Reliable File Transfer** | Truyền file UDP tin cậy, cắt chunk, đối soát toàn vẹn. | [`protocol/udp_stream.py`](protocol/udp_stream.py): Selective Repeat ARQ, cửa sổ trượt $N=8$, chunk 1200B, mã hash SHA-256. | 1 đ |
| **Step 7: Resilience & Error Handling** | Chịu lỗi sập nguồn, timeout socket, không crash server. | Bắt lỗi `ConnectionResetError`, `BrokenPipeError`; Watchdog quét định kỳ 5s đá user im lặng > 15s. | 1 đ |
| **Step 8: Security Layer** | Mã hóa dữ liệu E2EE trước khi gửi và giải mã khi nhận. | [`protocol/crypto.py`](protocol/crypto.py): Thuật toán chuẩn **AES-256-GCM** (khóa 32B, IV 12B), chống Replay Attack. | 1 đ |
| **Step 9: HTTP-based UI with Client JS** | Giao diện Web hiển thị chat, tiến trình tải, danh bạ online. | [`client/web_server.py`](client/web_server.py): Server HTTP/1.1 socket thuần phục vụ giao diện Web Dashboard, không dùng framework. | 1 đ |
| **Step 10: Testing & Code Quality** | Kiểm thử tự động hóa toàn diện, code sạch sẽ. | 14 bài test tự động hóa toàn diện (`tests/`): kiểm tra giao thức nhị phân, UDP streaming, REST API và kiểm thử tích hợp 15 clients. | 1 đ |

---

### Đối chiếu Đặc tả Kỹ thuật CO3094 (Hybrid Chat & RESTful Specification)

Bên cạnh 10 tiêu chí Rubric của Assignment 1, hệ thống hiện thực đầy đủ các đặc tả trong tài liệu `CO3094_assignment_streaprous.pdf` (Mục 2.2, 2.3 & 2.4):
- **Giai đoạn Đăng ký & Định danh (Client-Server Tracker - Trang 7):** Quản lý trạng thái phiên đa luồng với `client_ip`, `client_port`, `udp_port`, `state`, `last_activity`.
- **Hỗ trợ danh mục RESTful APIs theo Mục 2.4 (Trang 11):**
  - `/login` & `/submit-info`: Xác thực đăng nhập (hỗ trợ cơ chế Cookie RFC 6265 với header `Set-Cookie`).
  - `/get-list` & `/add-list`: Truy vấn danh bạ các peer trực tuyến.
  - `/connect-peer`: Thiết lập kết nối giữa client và mạng lưới.
  - `/broadcast-peer`: Gửi tin nhắn Broadcast đến toàn bộ mạng lưới peer.
  - `/send-peer`: Gửi tin nhắn Unicast trực tiếp đến một peer xác định.
  - `/send-file`: Truyền tệp nhị phân tin cậy qua UDP streaming với Selective Repeat ARQ.
- **Tiêu chuẩn mã nguồn (Mục 3.1):** Tuân thủ quy chuẩn định dạng PEP 8 và docstring chuẩn PEP 257.

---

## 3. Cấu trúc Thư mục Dự án

```text
assignment1/
├── protocol/                           # [Tầng Giao thức Ứng dụng]
│   ├── packet.py                      # Header TCP nhị phân 20B, CRC32, recv_exact chống dính/xé gói
│   ├── crypto.py                      # Mã hóa đối xứng AES-256-GCM (khóa 32B, IV 12B)
│   └── udp_stream.py                  # Selective Repeat ARQ, Sliding Window N=8, chunk 1200B
├── server/                             # [Tầng Máy chủ Trung tâm]
│   ├── server.py                      # TCP Server đa luồng non-blocking, Watchdog định kỳ 5s
│   ├── user_manager.py                # Quản lý danh bạ online, kiểm soát phiên an toàn luồng (RLock)
│   └── run_server.py                  # Điểm khởi chạy Server dòng lệnh
├── client/                             # [Tầng Máy khách Client]
│   ├── client.py                      # Động cơ Dual-Plane (TCP Signaling + UDP Transfer)
│   ├── web_server.py                  # Server HTTP/1.1 bằng Socket thuần (REST API /api/* & CO3094)
│   ├── run_client.py                  # Khởi chạy Client Web UI Dashboard
│   ├── downloads/                     # Thư mục lưu tệp nhận qua UDP
│   └── web/                           # Giao diện Web Client (HTML5, Modern CSS, Vanilla JS)
├── tests/                              # [Bộ Kiểm thử Tự động hóa Toàn diện]
│   ├── test_protocol.py               # Kiểm tra Header 20B, CRC32, mã hóa AES-GCM
│   ├── test_udp_stream.py             # Kiểm tra Selective Repeat ARQ và toàn vẹn SHA-256
│   └── test_integration.py            # Kiểm tra 15 clients đồng thời, ngắt socket đột ngột RST, REST API
└── README.md                          # Hướng dẫn chi tiết vận hành và đối chiếu Rubric
```

---

## 4. Yêu cầu Hệ thống & Cài đặt

- **Môi trường:** Python 3.10 trở lên (Windows, macOS, Linux).
- **Cài đặt thư viện:** Dự án chỉ sử dụng duy nhất một thư viện mật mã chuẩn theo yêu cầu:
  ```bash
  pip install cryptography
  ```

---

## 5. Hướng dẫn Vận hành Hệ thống

### Cách 1: Chạy nhiều Client trên cùng 1 máy tính

1. **Khởi động Máy chủ Trung tâm (Terminal 1):**
   ```bash
   python -m server.run_server
   ```
   *(Server lắng nghe trên `0.0.0.0` với TCP port `8888` và UDP port `8889`).*

2. **Khởi động Client Alice (Terminal 2):**
   ```bash
   python -m client.run_client Alice
   ```
   *(Hệ thống tự khởi chạy máy chủ Web socket thuần tại `http://127.0.0.1:5001`, đăng nhập `Alice` và tự động mở trình duyệt).*

3. **Khởi động Client Bob (Terminal 3):**
   ```bash
   python -m client.run_client Bob
   ```
   *(Hệ thống tự động chuyển sang cổng tiếp theo `http://127.0.0.1:5002`, đăng nhập `Bob` và tự động mở trình duyệt).*

4. **(Tùy chọn) Khởi động không cần truyền tên trước:**
   ```bash
   python -m client.run_client
   ```
   *(Trình duyệt sẽ mở Web UI, bấm nút **Connect** ở góc trên bên phải để nhập tên và server IP).*

---

### Cách 2: Vận hành trên 2 Máy tính khác nhau (Mạng LAN / Wi-Fi)

1. **Trên Máy 1 (chạy Server & Alice):**
   - Mở Terminal gõ `ipconfig` (hoặc `ifconfig` trên Linux/macOS) lấy địa chỉ IPv4 (ví dụ: `192.168.1.15`).
   - Khởi động Server:
     ```bash
     python -m server.run_server
     ```
   - Khởi động Client Alice:
     ```bash
     python -m client.run_client Alice
     ```

2. **Trên Máy 2 (Client Bob kết nối tới Máy 1):**
   - Chạy lệnh kết nối tới IP của Máy 1:
     ```bash
     python -m client.run_client Bob --host 192.168.1.15
     ```
   *(Trình duyệt trên Máy 2 sẽ mở Web UI và kết nối trực tiếp đến Server tại `192.168.1.15:8888`).*

---

### Hướng dẫn Sử dụng Giao diện Web UI

- **Global Chat (🌐):** Chọn kênh *Global Chat* ở thanh bên trái, nhập tin nhắn để gửi cho toàn bộ người dùng đang online.
- **Direct Message (💬):** Nhấp chuột vào tên một người dùng cụ thể trong mục *DIRECT MESSAGES* để gửi tin nhắn riêng tư End-to-End Encrypted.
- **UDP File Streaming (📁):** 
  - Tại bảng bên phải, chọn người nhận trong menu *Recipient Peer*.
  - Bấm *Select File* để chọn tệp cần gửi (hỗ trợ mọi định dạng tệp).
  - Bấm *Start UDP Streaming Transfer*: thanh tiến trình sẽ cập nhật từ $0\%$ đến $100\%$ theo thời gian thực.
  - Phía người nhận sẽ xuất hiện nút **Open** để mở tệp ngay trên trình duyệt (tệp được lưu tại `client/downloads/`).

---

## 6. Chạy Kiểm thử Tự động (Automated Test Suite)

Dự án cung cấp bộ kiểm thử gồm **14 bài test tự động** kiểm chứng 100% các tiêu chí kỹ thuật:

```bash
# Chạy toàn bộ 14 bài test cùng lúc (Khuyên dùng):
python -m unittest discover tests
```

Hoặc chạy độc lập từng bộ test:
```bash
# 1. Kiểm thử đóng gói Header 20B, CRC32 và mã hóa AES-256-GCM (Step 1 & 8)
python -m unittest tests/test_protocol.py

# 2. Kiểm thử truyền file Selective Repeat ARQ và đối soát SHA-256 (Step 6)
python -m unittest tests/test_udp_stream.py

# 3. Kiểm thử tải đồng thời 15 clients, chịu lỗi sập nguồn và REST API CO3094 (Step 2, 3, 4, 5, 7, 9)
python tests/test_integration.py
```

**Kết quả kiểm thử:**
```text
..............
----------------------------------------------------------------------
Ran 14 tests in 5.624s

OK
```
