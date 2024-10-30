# Basis-Image (Node.js für ARM-kompatible Umgebung)
FROM node:14-alpine

# Arbeitsverzeichnis festlegen
WORKDIR /app

# Abhängigkeiten kopieren und installieren
COPY package.json package-lock.json ./
RUN npm install --only=production

# Anwendungscode kopieren
COPY app.js ./

# Container ausführen
CMD ["node", "app.js"]
