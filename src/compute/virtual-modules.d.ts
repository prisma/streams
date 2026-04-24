declare module "virtual:prebuilt-studio-assets" {
  const mod: {
    appScript: string;
    appStyles: string;
    builtAssets: Map<
      string,
      {
        bytes: Uint8Array;
        contentType: string;
      }
    >;
  };

  export = mod;
}
