FROM apache/superset:6.0.0

USER root

# Optional: for Alerts & Reports (Playwright)
ENV PLAYWRIGHT_BROWSERS_PATH=/usr/local/share/playwright-browsers

# Install dependencies using system pip (Superset ≥3.0 does NOT use /app/.venv)
RUN pip install --no-cache-dir \
    psycopg2-binary \
    Authlib \
    openpyxl \
    Pillow \
    flask-cors


# OPTIONAL — install Playwright (for Alerts & Reports screenshots)
# Comment out this block if not needed
RUN pip install playwright && \
    playwright install-deps && \
    PLAYWRIGHT_BROWSERS_PATH=/usr/local/share/playwright-browsers playwright install chromium

# Return to superset user
USER superset

CMD ["/app/docker/entrypoints/run-server.sh"]
