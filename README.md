# Airflow Weather ETL Pipeline
This project utilizes Apache Airflow to build an ETL (Extract, Transform, Load) pipeline that retrieves weather data from the Open-Meteo API and stores it in an AWS RDS PostgreSQL database. The pipeline is designed to run daily, automatically extracting real-time weather information, transforming the raw data into a structured format, and loading it into the database for further analysis. Taking advantage of Airflow’s task management capabilities, the process ensures efficient data ingestion and storage while maintaining scalability and automation. And since latitude and longitude data are used, the dataset inherently becomes spatial. To easily visualiza our data, the AWS RDS database can be connected to ArcGIS Pro or similar GIS applications, allowing for visualization and the creation of dashboards using the data stored in the database.

# Apache Airflow ile Hava Durumu Verisi İçin ETL Pipeline Oluşturulması
Bu proje, Apache Airflow kullanarak Open-Meteo API’den hava durumu verilerini alan ve AWS RDS PostgreSQL veritabanına kaydeden bir ETL (Extract, Transform, Load) süreci oluşturur. Pipeline günlük olarak çalışacak şekilde tasarlanmış olup, gerçek zamanlı hava durumu verilerini otomatik olarak çekmekte, ham veriyi yapılandırılmış bir formata dönüştürmekte ve analiz için veritabanına yüklemektedir. Airflow’un görev yönetim yeteneklerinden faydalanarak, süreç verimli veri alımı ve depolama sağlarken ölçeklenebilirliği ve otomasyonu da korur. Ayrıca, enlem ve boylam verileri kullanıldığı için veri kümesi doğal olarak konumsal bir yapıya sahiptir. Veriyi kolayca görselleştirmek için AWS RDS veritabanı, ArcGIS Pro veya benzeri GIS uygulamalarına bağlanabilir, böylece veriler görselleştirilebilir ve veritabanında depolanan bilgilerle yönetici panelleri (dashboard'lar) oluşturulabilir.
