with imd as (
    select
        date_trunc('month', hire_date) as hire_month,
        date_trunc('month', termination_date) as term_month,
        status
    from {{ ref('intermediate') }}
),

resigns as (
    select
        term_month as month,
        count(*) * -1 as num_resign
    from imd
    where status = 'Resign'
    group by term_month
),

hires as (
    select
        hire_month as month,
        count(*) as num_new_hires
    from imd
    group by hire_month
),

bouncing as (
    select
        hire_month as month,
        count(*) as bouncing_hires
    from imd
    where hire_month = term_month
    group by hire_month
),

combined as (
    select
        coalesce(r.month, h.month, b.month) as month,
        coalesce(r.num_resign, 0) as num_resign,
        coalesce(h.num_new_hires, 0) as num_new_hires,
        coalesce(b.bouncing_hires, 0) as bouncing_hires
    from resigns r
    full outer join hires h on r.month = h.month
    full outer join bouncing b on coalesce(r.month, h.month) = b.month
),

cum_change as (
    select
        month,
        num_resign,
        num_new_hires,
        bouncing_hires,
        sum(num_new_hires + num_resign) OVER (ORDER BY month) as cum_change
    from combined
),

cumulative as (
    select
        month as tanggal,
        num_resign,
        num_new_hires,
        cum_change,
        (num_new_hires + num_resign) as net_change,
        bouncing_hires,
        lag(cum_change, 1, 0) OVER (ORDER BY month) as prev_cum_change,
    from cum_change
)

select
    tanggal,
    num_resign,
    num_new_hires,
    cum_change,
    net_change,
    bouncing_hires,
    prev_cum_change,
    'Monthly' as dim
from cumulative
order by 1


-- Penjelasan kolom hasil akhir:
-- tanggal: periode bulan ditulis dalam format tanggal di ISO 8601 dan selalu ditulis sebagai awal bulan
-- num_resign: jumlah pegawai yang resign, ditulis sebagai bilangan negatif
-- num_new_hires: jumlah pegawai baru, ditulis sebagai bilangan positif
-- cum_change: jumlah pegawai pada bulan tersebut setelah menghitung num_resign dan num_new_hires
-- net_change: hasil kalkulasi num_resign dan num_new_hires
-- bouncing_hires: jumlah orang yang resign pada bulan yang sama saat direkrut
-- prev_cum_change: nilai cum_change pada bulan sebelumnya
-- dim: kategorisasi periode waktu
