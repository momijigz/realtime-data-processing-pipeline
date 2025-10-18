FROM node:20-alpine

WORKDIR /app

# copy package files first to leverage layer caching
COPY package.json package-lock.json* ./

RUN npm ci --no-audit --no-fund || npm install --no-audit --no-fund

COPY . .

RUN npm run build

EXPOSE 3000

CMD ["node", "dist/index.js"]
