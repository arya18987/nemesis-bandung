echo '#!/bin/bash
pip install streamlit
streamlit run streamlit_app.py --server.port=8080 --server.address=0.0.0.0' > start.sh