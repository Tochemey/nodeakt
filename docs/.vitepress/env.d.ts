declare module "*.css" {}

declare module "*.vue" {
  const component: import("vitepress/theme").default["Layout"];
  export default component;
}
