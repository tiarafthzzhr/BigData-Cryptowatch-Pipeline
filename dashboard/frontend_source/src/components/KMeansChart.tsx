import { KMeansCluster } from '../utils/types';

const CLUSTER_LABELS: Record<number, { label: string; color: string; emoji: string }> = {};

function getClusterMeta(clusters: KMeansCluster[]) {
  // Sort by avg_price descending to assign labels
  const sorted = [...clusters].sort((a, b) => b.avg_price - a.avg_price);
  const labels = ['High Value', 'Mid Value', 'Low Value'];
  const colors = ['#f59e0b', '#06b6d4', '#10b981'];
  const emojis = ['🥇', '🥈', '🥉'];
  const meta: Record<number, { label: string; color: string; emoji: string }> = {};
  sorted.forEach((c, i) => {
    meta[c.prediction] = {
      label: labels[i] || `Cluster ${c.prediction}`,
      color: colors[i] || '#94a3b8',
      emoji: emojis[i] || '📊',
    };
  });
  return meta;
}

function formatPrice(price: number) {
  if (price >= 10000) return `$${(price / 1000).toFixed(1)}K`;
  if (price >= 1000) return `$${price.toFixed(0)}`;
  return `$${price.toFixed(2)}`;
}

export default function KMeansChart({ data, loading }: { data: KMeansCluster[]; loading: boolean }) {
  if (loading) {
    return (
      <div className="rounded-2xl bg-slate-900/80 border border-slate-700/50 p-6">
        <div className="h-6 w-48 bg-slate-800 rounded animate-pulse mb-4" />
        <div className="h-48 bg-slate-800/50 rounded-lg animate-pulse" />
      </div>
    );
  }

  if (!data || data.length === 0) {
    return (
      <div className="rounded-2xl bg-slate-900/80 border border-slate-700/50 p-6">
        <h2 className="text-lg font-semibold text-white mb-1 flex items-center gap-2">
          <div className="w-1.5 h-5 rounded-full bg-violet-400" />
          K-Means Clustering (MLlib)
        </h2>
        <p className="text-xs text-slate-500 mb-5">Menunggu data dari Spark MLlib...</p>
        <div className="text-center py-8 text-slate-600">
          <p className="text-sm">Data clustering belum tersedia.</p>
          <p className="text-xs mt-1">Spark akan memproses data secara otomatis setiap 2 menit.</p>
        </div>
      </div>
    );
  }

  const meta = getClusterMeta(data);
  const totalPoints = data.reduce((sum, c) => sum + c.jumlah_titik, 0);
  const maxPrice = Math.max(...data.map(c => c.avg_price));

  return (
    <div className="rounded-2xl bg-slate-900/80 border border-slate-700/50 p-6 backdrop-blur-sm">
      <h2 className="text-lg font-semibold text-white mb-1 flex items-center gap-2">
        <div className="w-1.5 h-5 rounded-full bg-violet-400" />
        K-Means Clustering (MLlib)
      </h2>
      <p className="text-xs text-slate-500 mb-5">
        Unsupervised learning — {data.length} clusters, {totalPoints} data points
      </p>

      <div className="space-y-4">
        {data
          .sort((a, b) => b.avg_price - a.avg_price)
          .map((cluster) => {
            const m = meta[cluster.prediction];
            const barWidth = maxPrice > 0 ? (cluster.avg_price / maxPrice) * 100 : 0;

            return (
              <div key={cluster.prediction} className="group">
                <div className="flex items-center justify-between mb-1.5">
                  <div className="flex items-center gap-2">
                    <span className="text-base">{m.emoji}</span>
                    <span className="text-sm font-semibold text-white">{m.label}</span>
                    <span
                      className="text-[10px] font-medium px-1.5 py-0.5 rounded-full"
                      style={{ backgroundColor: m.color + '22', color: m.color }}
                    >
                      Cluster {cluster.prediction}
                    </span>
                  </div>
                  <div className="text-right">
                    <span className="text-sm font-bold text-white">{formatPrice(cluster.avg_price)}</span>
                    <span className="text-[10px] text-slate-500 ml-2">{cluster.jumlah_titik} pts</span>
                  </div>
                </div>
                <div className="w-full bg-slate-800/60 rounded-full h-3 overflow-hidden">
                  <div
                    className="h-full rounded-full transition-all duration-700 ease-out"
                    style={{
                      width: `${barWidth}%`,
                      background: `linear-gradient(90deg, ${m.color}88, ${m.color})`,
                    }}
                  />
                </div>
              </div>
            );
          })}
      </div>

      <div className="mt-5 pt-4 border-t border-slate-800/60">
        <p className="text-[11px] text-slate-500 leading-relaxed">
          <span className="text-violet-400 font-medium">Algoritma K-Means</span> secara otomatis mengelompokkan aset kripto ke dalam{' '}
          {data.length} cluster berdasarkan profil harga dan volatilitasnya.
          Sistem ini siap untuk meng-cluster ratusan koin secara <span className="text-violet-400">scalable</span>.
        </p>
      </div>
    </div>
  );
}
