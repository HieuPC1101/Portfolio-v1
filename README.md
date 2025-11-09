# Portfolio-v1

Ứng dụng tối ưu hóa danh mục đầu tư chứng khoán Việt Nam sử dụng Streamlit.

## Giới thiệu
Portfolio-v1 là một hệ thống hỗ trợ nhà đầu tư phân tích, tối ưu hóa và quản lý danh mục đầu tư chứng khoán. Ứng dụng tích hợp các mô hình toán học, phân tích kỹ thuật, và giao diện trực quan để giúp người dùng ra quyết định đầu tư hiệu quả.

## Tính năng chính
- **Thu thập dữ liệu**: Tự động lấy thông tin công ty, ngành, giá cổ phiếu từ các nguồn dữ liệu Việt Nam.
- **Phân tích thị trường & ngành**: Hiển thị tổng quan thị trường, heatmap, drill-down theo ngành/sàn.
- **Tối ưu hóa danh mục**: Hỗ trợ các mô hình Markowitz, Max Sharpe, Min Volatility, Min CVaR, Min CDaR, HRP.
- **Phân tích kỹ thuật**: Tính toán các chỉ báo như SMA, EMA, RSI, MACD, Bollinger Bands.
- **Backtesting**: Kiểm tra hiệu quả danh mục đầu tư qua các giai đoạn lịch sử.
- **Quản lý phiên làm việc**: Lưu trữ trạng thái, bộ lọc, danh sách cổ phiếu đã chọn.
- **Giao diện trực quan**: Streamlit + Plotly, dễ sử dụng, thao tác nhanh.

## Cài đặt
1. Clone dự án:
	```powershell
	git clone https://github.com/HieuPC1101/Portfolio-v1.git
	```
2. Cài đặt các thư viện Python:
	```powershell
	pip install -r requirements.txt
	```
3. Chạy ứng dụng:
	```powershell
	streamlit run scripts/dashboard.py

## Yêu cầu hệ thống
- Python >= 3.8
- Kết nối Internet để lấy dữ liệu từ API

## Tài liệu & Hướng dẫn sử dụng
1. Chạy ứng dụng và truy cập giao diện web Streamlit.
2. Chọn các tham số phân tích, danh mục cổ phiếu, mô hình tối ưu hóa.
3. Xem kết quả phân tích, biểu đồ, backtest và xuất danh mục đầu tư.


