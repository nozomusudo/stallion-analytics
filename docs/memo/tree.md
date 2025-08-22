# 基本的な構造表示
tree

# より詳細（深度制限付き）
tree -L 3

# 隠しファイルも含めて表示
tree -a -L 3

# 特定のファイルを除外して表示
tree -I 'node_modules|venv|.git'

# 最もみやすいやつ
tree -I 'node_modules|venv|.git|.next|dist|build' -L 4



---

### 20250822

.
├── README.md
├── data
│   └── row
├── docs
│   └── memo
│       └── tree.md
├── package-lock.json
├── package.json
└── packages
    ├── bloodline-viz
    │   ├── README.md
    │   ├── eslint.config.mjs
    │   ├── next-env.d.ts
    │   ├── next.config.ts
    │   ├── package.json
    │   ├── postcss.config.mjs
    │   ├── public
    │   │   ├── file.svg
    │   │   ├── globe.svg
    │   │   ├── next.svg
    │   │   ├── vercel.svg
    │   │   └── window.svg
    │   ├── src
    │   │   └── app
    │   └── tsconfig.json
    ├── ml-analysis
    │   ├── data
    │   ├── notebooks
    │   ├── outputs
    │   ├── requirements.txt
    │   └── src
    ├── prediction-app
    │   ├── README.md
    │   ├── eslint.config.mjs
    │   ├── next-env.d.ts
    │   ├── next.config.ts
    │   ├── package.json
    │   ├── postcss.config.mjs
    │   ├── public
    │   │   ├── file.svg
    │   │   ├── globe.svg
    │   │   ├── next.svg
    │   │   ├── vercel.svg
    │   │   └── window.svg
    │   ├── src
    │   │   └── app
    │   └── tsconfig.json
    └── shared
        ├── package.json
        ├── src
        │   ├── database
        │   ├── types
        │   └── utils
        └── tsconfig.json
