FROM node:18 AS build
WORKDIR /app
COPY package*.json ./
COPY angular.json tsconfig*.json ./
RUN npm install
COPY src ./src
RUN npm run build -- --configuration production

FROM nginx:alpine
COPY --from=build /app/dist/md5-cracker-ui /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]

