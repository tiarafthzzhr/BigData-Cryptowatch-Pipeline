#!/bin/bash
cd /home/arkano/belajar_bigdata/ets-bigdata

# Hapus static assets lama
rm -rf dashboard/static/assets

# Copy assets baru dari dist
cp -r dashboard/frontend_source/dist/assets dashboard/static/assets

# Baca nama file JS dan CSS baru dari dist
JS_FILE=$(ls dashboard/frontend_source/dist/assets/index-*.js | head -1 | xargs basename)
CSS_FILE=$(ls dashboard/frontend_source/dist/assets/index-*.css | head -1 | xargs basename)

echo "JS: $JS_FILE"
echo "CSS: $CSS_FILE"

# Update index.html template dengan nama file baru
cat > dashboard/templates/index.html << 'HTMLEOF'
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <link rel="icon" type="image/svg+xml" href="/static/vite.svg" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>CryptoWatch — Big Data Dashboard</title>
    <meta name="description" content="Real-time Crypto Big Data Pipeline Dashboard" />
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet" />
HTMLEOF

# Append JS and CSS lines with variables
echo "    <script type=\"module\" crossorigin src=\"/static/assets/$JS_FILE\"></script>" >> dashboard/templates/index.html
echo "    <link rel=\"stylesheet\" crossorigin href=\"/static/assets/$CSS_FILE\">" >> dashboard/templates/index.html

cat >> dashboard/templates/index.html << 'HTMLEOF'
  </head>
  <body>
    <div id="root"></div>
  </body>
</html>
HTMLEOF

echo "Deploy complete!"
ls -la dashboard/static/assets/
cat dashboard/templates/index.html
