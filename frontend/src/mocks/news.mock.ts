import type { NewsFeedData } from "@/types/news";

export const newsMock: NewsFeedData = {
  items: [
    {
      id: "1",
      title: "VN-Index vượt mốc 1.280 điểm, dòng tiền ngoại quay trở lại",
      summary: "Thị trường chứng khoán Việt Nam tiếp tục đà tăng phiên thứ 3 liên tiếp...",
      source: "CafeF",
      time: "2 giờ trước",
      category: "Thị trường",
      url: "https://example.com/news/1",
    },
    {
      id: "2",
      title: "FPT đạt doanh thu kỷ lục quý I/2026, tăng trưởng 28% YoY",
      summary: "Tập đoàn FPT công bố kết quả kinh doanh quý I với doanh thu đạt 15,200 tỷ đồng...",
      source: "VnExpress",
      time: "4 giờ trước",
      category: "Doanh nghiệp",
      url: "https://example.com/news/2",
    },
    {
      id: "3",
      title: "Ngân hàng Nhà nước giữ nguyên lãi suất điều hành",
      summary: "NHNN quyết định giữ nguyên các mức lãi suất điều hành trong tháng 4/2026...",
      source: "VietStock",
      time: "6 giờ trước",
      category: "Vĩ mô",
      url: "https://example.com/news/3",
    },
    {
      id: "4",
      title: "HPG: Sản lượng thép tháng 3 giảm 5% do nhu cầu yếu",
      summary: "Hòa Phát ghi nhận sản lượng thép xây dựng giảm so với cùng kỳ...",
      source: "TCBS",
      time: "8 giờ trước",
      category: "Doanh nghiệp",
      url: "https://example.com/news/4",
    },
    {
      id: "5",
      title: "Khối ngoại mua ròng hơn 500 tỷ đồng trong tuần qua",
      summary: "Dòng vốn ngoại tích cực trở lại sau 3 tuần bán ròng liên tiếp...",
      source: "CafeF",
      time: "1 ngày trước",
      category: "Thị trường",
      url: "https://example.com/news/5",
    },
  ],
};
