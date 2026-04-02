"""
Module auto_optimization.py
Tự động chạy tất cả các mô hình tối ưu hóa và hiển thị kết quả so sánh.
"""

import streamlit as st
import logging
from backend.services.optimization_service import OptimizationRequest, get_optimization_service
from ui.optimization_comparison import render_optimization_comparison_tab
from utils.session_manager import save_optimization_result, get_optimization_results, clear_optimization_results

logger = logging.getLogger(__name__)


def run_all_models(data, total_investment, mode='manual'):
    """
    Chạy tất cả 6 mô hình tối ưu hóa và lưu kết quả.
    
    Args:
        data (pd.DataFrame): Dữ liệu giá cổ phiếu
        total_investment (float): Tổng số tiền đầu tư
        mode (str): 'manual' hoặc 'auto'
        
    Returns:
        dict: Kết quả của tất cả các mô hình
    """
    optimization_service = get_optimization_service()
    models = optimization_service.available_models()
    
    results = {}
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    total_models = len(models)
    
    for idx, (model_name, _) in enumerate(models.items(), 1):
        try:
            status_text.text(f" Đang chạy {model_name}... ({idx}/{total_models})")
            
            # Chạy mô hình
            result = optimization_service.run_model(
                model_name,
                OptimizationRequest(prices=data, total_investment=total_investment, mode=mode),
            )
            
            if result:
                # Lưu kết quả
                save_optimization_result(model_name, result, mode=mode)
                results[model_name] = result
                status_text.text(f" Hoàn thành {model_name}")
            else:
                status_text.text(f" Lỗi khi chạy {model_name}")
                logger.error(f"Không thể chạy {model_name}")
                
        except Exception as e:
            status_text.text(f" Lỗi {model_name}: {str(e)}")
            logger.error(f"Lỗi khi chạy {model_name}: {e}")
        
        # Cập nhật progress bar
        progress_bar.progress(idx / total_models)
    
    progress_bar.empty()
    status_text.empty()
    
    return results


def show_auto_optimization_results(data, total_investment, mode='manual'):
    """
    Hiển thị giao diện tự động chạy tất cả mô hình và so sánh kết quả.
    
    Args:
        data (pd.DataFrame): Dữ liệu giá cổ phiếu
        total_investment (float): Tổng số tiền đầu tư
        mode (str): 'manual' hoặc 'auto'
    """
    st.markdown("---")
    st.subheader(" Tối ưu hóa & So sánh Tự động")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.info("""
         **Chức năng này sẽ:**
        1. Tự động chạy cả 6 mô hình tối ưu hóa
        2. Lưu kết quả vào bộ nhớ
        3. Hiển thị bảng so sánh chi tiết
        4. Đưa ra khuyến nghị đầu tư tốt nhất
        """)
    
    with col2:
        st.metric("Số tiền đầu tư", f"{total_investment:,.0f} VND")
    
    # Nút chạy tất cả mô hình
    col_btn1, col_btn2 = st.columns(2)
    
    with col_btn1:
        run_button = st.button(
            " Chạy Tất cả Mô hình",
            type="primary",
            width='stretch',
            help="Chạy 6 mô hình tối ưu hóa một lượt"
        )
    
    with col_btn2:
        clear_button = st.button(
            " Xóa Kết quả Cũ",
            width='stretch',
            help="Xóa tất cả kết quả đã lưu"
        )
    
    if clear_button:
        clear_optimization_results(mode)
        st.success(" Đã xóa tất cả kết quả!")
        st.rerun()
    
    if run_button:
        with st.spinner(" Đang chạy tất cả các mô hình tối ưu hóa..."):
            # Xóa kết quả cũ trước khi chạy
            clear_optimization_results(mode)
            
            # Chạy tất cả mô hình
            results = run_all_models(data, total_investment, mode)
            
            if results:
                st.success(f" Hoàn thành! Đã chạy {len(results)}/{6} mô hình thành công.")
            else:
                st.error(" Không thể chạy bất kỳ mô hình nào. Vui lòng kiểm tra dữ liệu.")
    
    # Hiển thị kết quả nếu đã có
    existing_results = get_optimization_results(mode)
    
    if existing_results:
        st.markdown("---")
        st.markdown("###  Kết quả So sánh & Phân tích")
        
        # Hiển thị tab so sánh
        render_optimization_comparison_tab(existing_results)
    else:
        st.info(" Nhấn nút **'Chạy Tất cả Mô hình'** để bắt đầu tối ưu hóa và xem kết quả so sánh.")
