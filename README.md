# IF4044 Proyek Big Data Semester Genap 2025/2026
> oleh Kelompok 4

---

## Daftar Isi

- [Deskripsi](#deskripsi)
- [Prasyarat](#prasyarat)
- [Petunjuk Penggunaan](#petunjuk-penggunaan)
- [Struktur Folder](#struktur-folder)
- [Kreator](#kreator)

---

## Deskripsi

Proyek ini berfokus pada penyelesaian masalah inefisiensi penyimpanan dan lambatnya performa query analitik pada data CSV mentah dengan cara merancang data pipeline yang mencakup integrasi komponen Data Lakehouse, Data Orchestration, dan Data Mart.

---

## Prasyarat

- Docker Desktop
- Git
- Python 3.12
- ~15 GB penyimpanan bebas (untuk data TPC-H dan Docker images)

---

## Petunjuk Penggunaan

Cara menjalankan program dapat dilihat pada berkas [README.txt](Kode/README.txt)

---

## Struktur Folder

```
Proyek-Big-Data-IF4044/
├── Laporan/                          
└── Kode/
    ├── docker-compose.yml
    ├── requirements.txt
    ├── scripts/
    │   ├── download-jars.sh          
    │   └── upload-tpch-to-minio.sh   
    ├── spark/
    │   ├── conf/spark-defaults.conf  
    │   └── jars/                     
    ├── notebooks/
    │   ├── load_and_read.ipynb       
    │   └── test_connection.ipynb     
    ├── data/                         
    │   └── tpch-csv/                 
    └── dbt/                          
        ├── dbt_project.yml           
        ├── profiles.yml              
        ├── macros/
        │   └── net_revenue.sql       
        └── models/
            ├── staging/
            │   └── _sources.yml      
            └── marts/
                ├── revenue_by_segment_region.sql  
                └── _models.yml       
```

## Kreator
| NIM | Nama |
|-----|------|
| 13522033 | Bryan Cornelius Lauwrence |
| 13523013 | Nathaniel Jonathan Rusli |
| 13523086 | Bob Kunanda |