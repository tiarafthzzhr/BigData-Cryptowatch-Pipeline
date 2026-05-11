export interface CryptoPrice {
  symbol: string;
  price_usd: number;
  price_idr: number;
  change_24h: number;
  timestamp: string;
}

export interface NewsItem {
  title: string;
  link: string;
  pubDate: string;
  hash: string;
  source: string;
}

export interface HourlyPrice {
  hour: string;
  btc: number;
  eth: number;
  bnb: number;
}

export interface HourlyVolatility {
  hour: string;
  btc: number;
  eth: number;
  bnb: number;
}

export interface NewsVolume {
  hour: string;
  count: number;
}

export interface PipelineStep {
  name: string;
  label: string;
  status: 'active' | 'error';
  description: string;
}

export interface KMeansCluster {
  prediction: number;
  jumlah_titik: number;
  avg_price: number;
}
