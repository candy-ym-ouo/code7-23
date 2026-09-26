# 公共空间细节地图

一个真实可运行的公共空间 UGC 地图项目。用户可以记录长椅、饮水处、遮雨棚、安静角落和夜间照明，上传经过服务端隐私处理的照片，提交评论、举报和时效确认；所有内容审核通过后才会公开。

- 前端：Vue 3、TypeScript、Vite、MapLibre GL JS、Pinia。
- 后端：Node.js、Fastify、PostgreSQL/PostGIS、Redis/BullMQ。
- 媒体：MinIO/S3 私有隔离区与服务公开桶、Sharp 服务端模糊、ClamAV 扫描。
- 审核：投稿、评论、媒体隐私和举报均进入真实审核队列。
- 原则：没有 mock 数据、没有内存数据库、没有绕过审核的发布路径。

完整产品和实现规格见 [docs/项目文档.md](docs/项目文档.md)。

## 目录结构

```text
apps/
  api/       Node.js HTTP API
  web/       Vue 3 前端
  worker/    媒体隐私处理、邮件 outbox、定时维护
packages/
  shared/    共享 Zod 合约、分类定义、密码与令牌工具
  db/        数据库迁移、分类种子、管理员创建 CLI
infra/
  caddy/     生产反向代理
  minio/     私有桶、公开桶和浏览器 CORS
  nginx/     Web 静态资源
  postgres/  PostGIS 初始化
docs/        项目、API、隐私和运维文档
```

## 本地启动

要求：Node.js 22+、pnpm 11+，以及本地可用的 PostgreSQL/PostGIS、Redis、S3 兼容对象存储、SMTP 服务和 ClamAV。

```bash
cp .env.example .env
pnpm install
pnpm db:migrate
pnpm db:seed
pnpm admin:create
pnpm dev
```

访问地址：

- Web：<http://localhost:5173>
- API：<http://localhost:3000>
- API 就绪检查：<http://localhost:3000/health/ready>
- MinIO Console：<http://localhost:9001>
- Mailpit：<http://localhost:8025>

ClamAV 首次启动需要下载病毒库，可能耗时数分钟。只做非媒体开发时可在 `.env` 中设置 `CLAMAV_ENABLED=false`，媒体仍会经过服务端隐私模糊，但不会执行病毒扫描，不得用于生产。

`pnpm admin:create` 不接受默认密码，必须在交互终端中输入至少 12 个字符的密码。

## 真实使用流程

1. 注册账号，在 Mailpit 或配置的 SMTP 邮箱中打开验证链接。
2. 登录后在投稿页选择地图位置、分类和结构化细节。
3. 上传照片，在浏览器中框选人脸、车牌等敏感区域。
4. 浏览器将原图 PUT 到私有隔离桶，API 创建处理任务。
5. Worker 执行 ClamAV 扫描、EXIF 方向校正、元数据移除、服务端模糊、WebP 重编码和缩略图生成。
6. 未配置自动检测器时，媒体进入 `manual_review`。审核员查看短期签名预览并确认隐私后，媒体才转为 `ready`。
7. 投稿提交后进入 `pending`，管理员批准后才出现在公共地图。
8. 评论默认 `pending`；举报达到阈值会隐藏目标并进入审核。
9. 已发布内容修改时创建新修订，旧公开版本在新修订批准前持续可见。

## 验证

```bash
pnpm typecheck
pnpm test
pnpm build
```

`pnpm verify` 串联类型检查、测试和生产构建。

媒体故障注入集成测试（扫描超时、模糊失败、对象丢失、队列重放，含状态机/审计/最终一致性/步骤重放）需要本地真实的 PostgreSQL/PostGIS、Redis 和 S3 兼容存储：

```bash
pnpm test:integration
```

手工闭环验证建议：

```bash
curl http://localhost:3000/health/ready
curl 'http://localhost:3000/api/v1/features?bbox=116.30,39.80,116.50,40.00'
```

## 地图瓦片

开发环境默认使用 `VITE_TILE_URL`，未设置时使用 OpenStreetMap 公共瓦片。生产环境不能依赖公共瓦片，必须设置自托管或获授权的瓦片服务：

```env
VITE_TILE_URL=https://tiles.example.com/{z}/{x}/{y}.png
```

## 安全边界

- 访问令牌短期有效，刷新令牌放在 `HttpOnly` Cookie 中并轮换。
- 密码使用 Argon2id。
- 对象级权限在服务端重新检查，不能依靠前端路由。
- 私有原图不公开；审核预览使用短期签名 URL。
- 生产环境必须使用 HTTPS、强密钥、受控 SMTP 和备份。
- 媒体检测器未配置时不会自动发布，必须经过人工隐私确认。

## 关键文档

- [项目规格](docs/项目文档.md)
- [API 约定](docs/api.md)
- [隐私与媒体处理](docs/privacy.md)
- [部署与运维](docs/operations.md)
