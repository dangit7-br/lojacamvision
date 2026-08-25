FROM node:22-alpine

WORKDIR /app

COPY . .

RUN npm ci --omit=dev
RUN mkdir -p /app/data

ENV PORT=3000
ENV DATA_DIR=/app/data
ENV TZ=America/Sao_Paulo
ENV APP_TIMEZONE=America/Sao_Paulo

EXPOSE 3000

CMD ["node", "server.js"]
