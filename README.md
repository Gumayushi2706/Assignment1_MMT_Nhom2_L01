<p align="center">
  <img src="logo.png" width="120" alt="Trường ĐH Bách Khoa - ĐHQG TP.HCM Logo" />
</p>

# Assignment 1: Distributed Streaming Collaboration System
> **Môn học:** CO3093 / CO3094 – Mạng Máy Tính & Phát triển Ứng dụng Mạng (HCMUT)  
> **Dự án:** Ứng dụng cộng tác Client-Server và Peer-to-Peer thời gian thực dựa trên kết nối phân tán và streaming.

---

## 1. Giới thiệu

Hệ thống được xây dựng **100% bằng Socket thuần mức thấp** (`import socket`), hoàn toàn không sử dụng web framework bậc cao (Flask, Django, Tornado) hay WebSockets:
- **TCP Signaling Plane (Port 8888):** Bắt tay 2 chiều, xác thực đăng nhập duy nhất, quản lý danh bạ online (Peer Discovery) và nhắn tin E2EE thời gian thực (Broadcast & Unicast).
- **UDP Data Streaming Plane (Port 8889 / Dynamic):** Truyền file tin cậy độc lập với giải thuật **Selective Repeat ARQ** (cửa sổ trượt $N=8$, chunk 1200B $< 1500\text{B}$ MTU, đối soát SHA-256).
- **Bảo mật Tầng Ứng dụng:** Mã hóa xác thực toàn trình **AES-256-GCM** (khóa 32B, nonce 12B).
- **Web UI Socket thuần (Zero-Framework):** Tự xây dựng HTTP/1.1 server bằng socket phục vụ giao diện Web Dashboard, quản lý phiên Cookie RFC 6265, Basic Auth RFC 7235 và hiển thị thông báo tin nhắn chưa đọc.

---

## 2. Cài đặt

- **Yêu cầu môi trường:** Python 3.10 trở lên (Windows / macOS / Linux).
- **Cài đặt thư viện mật mã:**
  ```bash
  pip install cryptography
  ```

---

## 3. Hướng dẫn Chạy Hệ Thống

### Cách 1: Chạy trên cùng 1 máy tính (Localhost)

Mở 3 cửa sổ Terminal riêng biệt:

1. **Terminal 1 – Khởi động Server trung tâm:**
   ```bash
   python -m server.run_server
   ```
   *(Server lắng nghe tại `0.0.0.0:8888` cho TCP và `8889` cho UDP).*

2. **Terminal 2 – Khởi động Client Alice:**
   ```bash
   python -m client.run_client Alice
   ```
   *(Tự động mở trình duyệt Web Dashboard tại `http://127.0.0.1:5001`).*

3. **Terminal 3 – Khởi động Client Bob:**
   ```bash
   python -m client.run_client Bob
   ```
   *(Tự động mở trình duyệt Web Dashboard tại `http://127.0.0.1:5002`).*

*(Bạn có thể mở thêm nhiều client khác như Trudy, David... bằng lệnh `python -m client.run_client <Tên>` tương tự).*

---

### Cách 2: Chạy trên 2 máy tính khác nhau (Cùng mạng LAN / Wi-Fi)

1. **Trên Máy 1 (Server & Alice):**
   - Mở Terminal gõ `ipconfig` (hoặc `ifconfig` trên Linux/macOS) để lấy địa chỉ IPv4 (ví dụ: `192.168.1.15`).
   - Khởi động Server:
     ```bash
     python -m server.run_server
     ```
   - Khởi động Client Alice:
     ```bash
     python -m client.run_client Alice
     ```

2. **Trên Máy 2 (Client Bob kết nối sang Máy 1):**
   - Chạy lệnh kết nối tới IP Máy 1:
     ```bash
     python -m client.run_client Bob --host 192.168.1.15
     ```
   *(Trình duyệt trên Máy 2 sẽ tự động mở Web UI và kết nối trực tiếp đến Server tại `192.168.1.15:8888`).*

---

## 4. Hướng dẫn Sử dụng Giao diện Web UI

- **Chat công khai (Global Chat 🌐):** Chọn kênh *Global Chat* ở cột bên trái để gửi tin nhắn đến tất cả mọi người đang online.
- **Chat riêng tư (Direct Messages 💬):** Nhấp chọn một người dùng cụ thể (như Bob, Trudy) để gửi tin nhắn trực tiếp được mã hóa E2EE.
- **Hệ thống thông báo:** 
  - Tiêu đề tab trình duyệt tự động cập nhật tổng số tin nhắn chưa đọc: `(3) Assignment 1...`
  - Từng kênh/người gửi hiển thị huy hiệu đỏ chứa số lượng tin chưa đọc. Khi nhấp vào đọc, thông báo sẽ tự động biến mất.
- **Truyền file UDP Streaming (📁):** 
  - Tại bảng bên phải, chọn người nhận trong menu *Recipient Peer*.
  - Nhấp *Select File* để chọn tệp tin bất kỳ từ máy tính.
  - Bấm *Start UDP Streaming Transfer*: thanh tiến trình sẽ chạy mượt mà từ $0\%$ đến $100\%$.
  - Người nhận sẽ thấy nút **Open** để mở tệp ngay lập tức (tệp được lưu tại thư mục `client/downloads/`).

---

## 5. Chạy Kiểm Thử Tự Động (Unit Tests)

Bộ kiểm thử tự động gồm **15 bài test** kiểm tra toàn bộ giao thức nhị phân, giải thuật Selective Repeat ARQ dưới 20% mất gói ngẫu nhiên, mô phỏng tải 15 clients đồng thời và REST API:

```bash
python -m unittest discover tests -v
```

**Kết quả mong đợi:**
```text
Ran 15 tests ... OK
```

---

## 6. Cấu trúc Thư mục

```text
assignment1/
├── protocol/               # Giao thức TCP Framing 20B, CRC32, AES-256-GCM, UDP Selective Repeat
├── server/                 # Server đa luồng TCP/UDP, quản lý phiên và Watchdog tự động
├── client/                 # Client networking engine, Server HTTP socket thuần & Web Dashboard
│   ├── downloads/          # Thư mục lưu tệp nhận qua UDP
│   └── web/                # Giao diện Web UI (index.html, style.css, app.js)
├── tests/                  # Bộ 15 bài kiểm thử tự động hóa toàn diện
└── README.md               # Hướng dẫn cài đặt và vận hành
```
