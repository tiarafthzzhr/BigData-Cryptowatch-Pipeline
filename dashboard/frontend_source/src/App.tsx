import { useState, useEffect, useCallback } from 'react';
import Header from './components/Header';
import CryptoPriceTable from './components/CryptoPriceTable';
import PriceChart from './components/PriceChart';
import VolatilityChart from './components/VolatilityChart';
import NewsVolumeChart from './components/NewsVolumeChart';
import LatestNews from './components/LatestNews';
import PipelineStatus from './components/PipelineStatus';
import KMeansChart from './components/KMeansChart';
import { fetchAllData } from './utils/api';
import { CryptoPrice, HourlyPrice, HourlyVolatility, NewsVolume, NewsItem, PipelineStep, KMeansCluster } from './utils/types';

const REFRESH_INTERVAL = 60000;

function App() {
  const [prices, setPrices] = useState<CryptoPrice[]>([]);
  const [hourlyPrices, setHourlyPrices] = useState<HourlyPrice[]>([]);
  const [volatility, setVolatility] = useState<HourlyVolatility[]>([]);
  const [newsVolume, setNewsVolume] = useState<NewsVolume[]>([]);
  const [newsItems, setNewsItems] = useState<NewsItem[]>([]);
  const [pipeline, setPipeline] = useState<PipelineStep[]>([]);
  const [clusters, setClusters] = useState<KMeansCluster[]>([]);

  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchAllData();
      setPrices(data.prices);
      setHourlyPrices(data.hourlyPrices);
      setVolatility(data.volatility);
      setNewsVolume(data.newsVolume);
      setNewsItems(data.newsItems);
      setPipeline(data.pipeline);
      setClusters(data.clusters);
      setLastRefresh(new Date());
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, REFRESH_INTERVAL);
    return () => clearInterval(interval);
  }, [loadData]);

  return (
    <div className="min-h-screen bg-slate-950 text-white">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
        <Header />

        {lastRefresh && (
          <div className="flex items-center justify-between mb-6">
            <p className="text-xs text-slate-500">
              Last updated: {lastRefresh.toLocaleTimeString('en-US', { hour12: false })}
              <span className="ml-2 text-slate-600">| Auto-refresh: 60s</span>
            </p>
            <button
              onClick={loadData}
              className="text-xs text-cyan-400 hover:text-cyan-300 transition-colors font-medium"
            >
              Refresh Now
            </button>
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
          <div className="lg:col-span-2">
            <CryptoPriceTable data={prices} loading={loading} />
          </div>
          <div>
            <PipelineStatus steps={pipeline} loading={loading} />
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <PriceChart data={hourlyPrices} loading={loading} />
          <VolatilityChart data={volatility} loading={loading} />
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <NewsVolumeChart data={newsVolume} loading={loading} />
          <LatestNews items={newsItems} loading={loading} />
        </div>

        {/* K-Means Clustering (Bonus MLlib) */}
        <div className="grid grid-cols-1 gap-6">
          <KMeansChart data={clusters} loading={loading} />
        </div>

        <footer className="mt-10 pb-6 text-center">
          <p className="text-xs text-slate-600">
            Crypto Big Data Dashboard &mdash; Data sourced from CoinGecko API &amp; RSS Feeds
          </p>
        </footer>
      </div>
    </div>
  );
}

export default App;
