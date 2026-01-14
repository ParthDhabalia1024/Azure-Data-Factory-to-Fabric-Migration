"""
UI Configuration for OnPoint Insights ADF to Fabric Migration Tool
Matches OnPoint Insights brand colors, fonts, and design language
"""

import streamlit as st
from typing import Dict, Any
import os
from PIL import Image

# OnPoint Insights Brand Colors (Matching Logo)
PRIMARY_COLOR = "#005AB8"  # Deep Blue from logo
SECONDARY_COLOR = "#00A8E1"  # Light Blue/Cyan from logo circle
ACCENT_COLOR = "#0088D4"  # Medium Blue accent
LIGHT_BG = "#F5F9FC"  # Very light blue background
WHITE_BG = "#FFFFFF"  # White background
LIGHT_TEXT = "#FFFFFF"  # White text for contrast
DARK_TEXT = "#1A4D7A"  # Dark blue-gray text
GRAY_BG = "#E0EEF7"  # Light blue-gray for sections
CARD_BG = "#FFFFFF"  # White cards

# CSS Configuration for Streamlit
STREAMLIT_CUSTOM_CSS = f"""
<style>
    /* Main Background */
    .main {{
        background: linear-gradient(135deg, #F5F9FC 0%, #E8F4FA 100%);
        color: {DARK_TEXT};
    }}
    
    /* Sidebar Styling */
    [data-testid="stSidebar"] {{
        background: linear-gradient(180deg, #FFFFFF 0%, #F5F9FC 100%);
        border-right: 3px solid {SECONDARY_COLOR};
    }}
    
    /* Container Styling */
    [data-testid="stVerticalBlock"] > [data-testid="stVerticalBlock"] > [data-testid="stVerticalBlock"] {{
        background: {CARD_BG};
        border: 2px solid {SECONDARY_COLOR};
        border-radius: 10px;
        padding: 20px;
        margin: 10px 0;
        box-shadow: 0 2px 8px rgba(0, 102, 204, 0.1);
    }}
    
    /* Header Styling */
    h1 {{
        color: {SECONDARY_COLOR};
        font-weight: 700;
        font-size: 2.5em;
    }}
    
    h2 {{
        color: {PRIMARY_COLOR};
        font-weight: 600;
        margin-top: 20px;
        border-bottom: 3px solid {SECONDARY_COLOR};
        padding-bottom: 10px;
    }}
    
    h3 {{
        color: {PRIMARY_COLOR};
        font-weight: 500;
    }}
    
    /* Button Styling */
    .stButton > button {{
        background: linear-gradient(135deg, {PRIMARY_COLOR}, {SECONDARY_COLOR});
        color: #FFFFFF;
        border: none;
        border-radius: 8px;
        padding: 10px 24px;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(0, 102, 204, 0.25);
    }}
    
    .stButton > button:hover {{
        box-shadow: 0 6px 20px rgba(0, 102, 204, 0.4);
        transform: translateY(-2px);
    }}
    
    /* Input Fields */
    input, textarea, select {{
        background: {WHITE_BG} !important;
        color: {DARK_TEXT} !important;
        border: 2px solid {SECONDARY_COLOR} !important;
        border-radius: 6px !important;
    }}
    
    input::placeholder {{
        color: rgba(26, 26, 26, 0.5) !important;
    }}
    
    /* Info/Success/Error/Warning Messages */
    [data-testid="stAlert"] {{
        border-radius: 8px;
        border-left: 4px solid;
        padding: 15px;
        background: #FFFFFF;
        color: {DARK_TEXT};
    }}
    
    .stSuccess {{
        border-left-color: #00C853 !important;
        background: rgba(0, 200, 83, 0.08) !important;
    }}
    
    .stError {{
        border-left-color: {ACCENT_COLOR} !important;
        background: rgba(255, 107, 53, 0.08) !important;
    }}
    
    .stWarning {{
        border-left-color: #FFC107 !important;
        background: rgba(255, 193, 7, 0.08) !important;
    }}
    
    .stInfo {{
        border-left-color: {SECONDARY_COLOR} !important;
        background: rgba(0, 168, 225, 0.08) !important;
    }}
    
    /* Selectbox and Multiselect */
    [data-testid="stSelectbox"] > div > div {{
        background: {WHITE_BG};
        border: 2px solid {SECONDARY_COLOR};
        border-radius: 6px;
        color: {DARK_TEXT};
    }}
    
    /* Dataframe Styling */
    [data-testid="stDataFrame"] {{
        color: {DARK_TEXT};
    }}
    
    [data-testid="stDataFrame"] tbody tr:hover {{
        background-color: rgba(0, 168, 225, 0.08) !important;
    }}
    
    /* Caption and Text */
    .stCaption {{
        color: rgba(26, 26, 26, 0.7);
        font-size: 0.9em;
    }}
    
    /* Markdown Text */
    p {{
        color: {DARK_TEXT};
        line-height: 1.6;
    }}
    
    /* Divider */
    hr {{
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, {SECONDARY_COLOR}, transparent);
        margin: 20px 0;
    }}
    
    /* Spinner Text */
    [data-testid="stSpinner"] {{
        color: {SECONDARY_COLOR};
    }}
    
    /* Code Block */
    code {{
        background: rgba(0, 102, 204, 0.08);
        color: {PRIMARY_COLOR};
        border-radius: 4px;
        padding: 2px 6px;
    }}
</style>
"""


def apply_custom_theme() -> None:
    """Apply OnPoint Insights custom theme to Streamlit app"""
    st.markdown(STREAMLIT_CUSTOM_CSS, unsafe_allow_html=True)


def render_header_with_logo(title: str, subtitle: str = "", logo_path: str = None) -> None:
    """Render custom header with OnPoint Insights logo and branding"""
    col1, col2 = st.columns([0.7, 4.3])
    
    with col1:
        # Add spacing to align logo with heading
        st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)
        logo_displayed = False
        if logo_path:
            try:
                # Try to load and display the logo
                from pathlib import Path
                logo_file = Path(logo_path)
                
                # Debug: check file existence
                if logo_file.exists():
                    logo = Image.open(str(logo_file))
                    # Display logo with smaller size
                    st.image(logo, width=150)
                    logo_displayed = True
                else:
                    # Try without absolute path conversion
                    if os.path.exists(logo_path):
                        logo = Image.open(logo_path)
                        st.image(logo, width=150)
                        logo_displayed = True
            except Exception as e:
                st.write(f"Logo error: {e}")
        
        if not logo_displayed:
            st.markdown("# 🔷")  # Fallback icon
    
    with col2:
        st.markdown(f"""
        <h1 style="margin: 0; padding-top: 15px; color: #00A8E1; font-size: 2.3em; font-weight: 700; line-height: 1.2;">
            {title}
        </h1>
        <p style="margin: 8px 0 0 0; color: #1A4D7A; font-style: italic; font-size: 0.95em;">
            {subtitle}
        </p>
        """, unsafe_allow_html=True)


def render_header(title: str, subtitle: str = "") -> None:
    """Render custom header with OnPoint Insights branding"""
    col1, col2 = st.columns([1, 4])
    with col1:
        st.markdown("🔷")  # OnPoint Insights accent
    with col2:
        st.markdown(f"# {title}")
        if subtitle:
            st.markdown(f"*{subtitle}*")


def render_info_box(title: str, content: str, icon: str = "ℹ️") -> None:
    """Render custom info box with brand styling"""
    with st.container(border=True):
        st.markdown(f"### {icon} {title}")
        st.markdown(content)


def render_success_box(title: str, content: str) -> None:
    """Render custom success box"""
    with st.container(border=True):
        st.markdown(f"### ✅ {title}")
        st.markdown(content)


def get_color_palette() -> Dict[str, str]:
    """Get OnPoint Insights color palette"""
    return {
        "primary": PRIMARY_COLOR,
        "secondary": SECONDARY_COLOR,
        "accent": ACCENT_COLOR,
        "dark_bg": DARK_BG,
        "light_text": LIGHT_TEXT,
        "gray_bg": GRAY_BG,
        "dark_text": DARK_TEXT,
    }


def style_metric(label: str, value: str, delta: str = "") -> None:
    """Render styled metric display"""
    col1, col2 = st.columns([2, 1])
    with col1:
        st.markdown(f"**{label}**")
        st.markdown(f"### {value}")
    with col2:
        if delta:
            st.markdown(f"*{delta}*")
