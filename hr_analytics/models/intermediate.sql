-- clean data from staging

select 
    case
        when lower(NIK_KARYAWAN) = 'nan' then null
        else cast(NIK_KARYAWAN as varchar)
    end as NIK_KARYAWAN
    , PERUSAHAAN
    , HIRE_DATE
    , TERMINATION_DATE
    , STATUS
from {{ ref('staging') }}
where 
    ((NIK_KARYAWAN is not null) or (not lower(NIK_KARYAWAN) = 'nan'))
    and (PERUSAHAAN is not null)
    and (HIRE_DATE is not null)
    and (TERMINATION_DATE is not null)
    and (STATUS is not null)