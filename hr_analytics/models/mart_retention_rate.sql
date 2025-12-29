with hc_change as (
    select *
    from {{ ref('mart_hc_change') }}
),

retention as (
    select
        tanggal,
        lag(cum_change) over (order by tanggal) as prev_cum_change,
        (num_resign + bouncing_hires) as retaining
    from hc_change
),

final as (
    select
        r.tanggal,
        r.prev_cum_change,
        r.retaining,
        case
            when r.prev_cum_change = 0 then null
            else round((r.retaining::decimal / r.prev_cum_change) * 100, 2)
        end as retention_rate
    from retention r
)

select
    f.tanggal,
    f.prev_cum_change,
    f.retaining,
    f.retention_rate,
    'Monthly' as dim
from final f
order by 1


-- Penjelasan kolom hasil akhir:
-- tanggal: periode bulan ditulis dalam format tanggal di ISO 8601 dan selalu ditulis sebagai awal bulan
-- prev_cum_change: nilai cum_change pada bulan sebelumnya (lihat metrik headcount_change)
-- retaining: hasil kalkulasi num_resign dan bouncing_hires (lihat metrik headcount_change)
-- retention_rate: hasil kalkulasi prev_cum_change dan retaining
-- dim: kategorisasi periode waktu
