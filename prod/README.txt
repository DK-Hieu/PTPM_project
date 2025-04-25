Link database UAT:
https://drive.google.com/file/d/14I57gu1MlxE0i1pCRF_3qPkKuUGuVuCr/view?usp=drive_link

Link dataset Movie_info_sample
https://drive.google.com/file/d/14YlnMZ7GH21WGLhzvGghaMihdSYAWsFY/view?usp=sharing

Link EDR:
https://dbdiagram.io/d/PTPM_ERD-67f24e484f7afba184817c54

Metadata:

Table stg_links
{
  movieid   bigint [pk,note:'ID phim do grouplen đặt']
  imdbid    bigint [note:'Mã id của phim trong imdb database']
  tmdbid    bigint [note:'Mã id của phim trong tmdb database']
}

Table stg_keyword  
{
  keyid     varchar(5000)   [pk, note:'pk = userid + movieid + timestamp_']
  userid    bigint          [note:'ID của user']
  movieid   bigint          [note:'Grouplen movie ID']
  keywords  varchar(5000)   [note:'Tag do user đặt']
  date_tags TIMESTAMP       [note:'Thời gian vote']
}

Ref: stg_links.movieid < stg_keyword.movieid

Table stg_ratings  
{
  keyid     varchar(5000)   [pk,note:'pk = userid + movieid + timestamp_']
  userid    bigint          [note:'Mã ID của user']
  movieid   bigint          [note:'ID phim do grouplen đặt']
  rating    int             [note:'Điểm rating user vote']
  date_rate TIMESTAMP       [note:'Thời gian vote']
}

Ref: stg_links.movieid < stg_ratings.movieid

Table stg_user
{
  userid    bigint          [pk,note:'Mã ID user']
  fullname  varchar(5000)   [note:'Tên user']
  age       int             [note:'Tuổi']
  address   varchar(5000)   [note:'Địa chỉ']
}

Ref: stg_user.userid < stg_keyword.userid

Ref: stg_user.userid < stg_ratings.userid

Table stg_movie_metadata
{
  id                  bigint          [pk, note:'TMDB ID của phim'] 
  title               varchar(500)    [note:'Tiêu đề phim']
  original_title      varchar(500)    [note:'Tiêu đề gốc']
  original_language   varchar(500)    [note:'Ngôn ngữ gốc']
  release_date        date            [note:'Ngày phát hành']
  status              varchar(500)    [note:'Trạng thái phát hành']
  overview            varchar(5000)   [note:'Nội dung tóm tắt']
  tagline             varchar(5000)   [note:'Câu tóm tắt']
  adult               varchar(500)    [note:'Phân loại phim 18+']
  popularity          float           [note:'Độ phổ biến']
  homepage            varchar(500)    [note:'Trang chủ của phim']
  poster_path         varchar(500)    [note:'Link poster']
  runtime             int             [note:'Thời lượng phim']
  budget              bigint          [note:'Kinh phí']
  revenue             bigint          [note:'Doanh thu']
  vote_average        float           [note:'Điểm trung bình vote']
  vote_count          int             [note:'Số lượng vote']
}

Ref: stg_movie_metadata.id < stg_links.tmdbid

Ref: stg_movie_metadata.id < stg_cast.id

Ref: stg_movie_metadata.id < stg_crew.id

Table stg_cast
{
  credit_id     varchar(500)          [pk, note:'credit id hệ thống']
  id            bigint                [note:'TMDB ID của phim']
  actor_id      bigint                [note:'TMDB ID diễn viên']
  cast_id       int                   [note:'TMDB ID cast (ID Nhân vật)']
  character     varchar(500)          [note:'Tên nhân vật']
}

Table stg_crew
{
  credit_id     varchar(500)          [pk, note: 'credit id hệ thống']
  id            bigint                [note:'TMDB ID của phim']
  crew_id       bigint                [note:'TMDB ID nhân viên đoàn phim']
  department    varchar(500)          [note:'Vị trí làm việc']
  job           varchar(500)          [note:'Nghề nghiệp']
}

Table stg_person
{
  id                bigint            [pk,note:'TMDB ID diễn viên hoặc đoàn phim']
  imdb_id           varchar(500)      [note:'TMDB ID của phim']
  fullname          varchar(256)      [note:'Tên đầy đủ']
  also_name         varchar(5000)     [note:'Nghệ danh, Tên khác']
  gender            int               [note:'Giới tính']
  birthday          date              [note:'Ngày sinh']
  deathday          date              [note:'Ngày mất']
  place_of_birth    varchar(5000)     [note:'Nơi sinh']
  job               varchar(5000)     [note:'Nghề nghiệp']
  popularity        float             [note:'Độ phổ biến']
  profile_path      varchar(5000)     [note:'Link hình ảnh']
  biography         text              [note:'Tiểu sử']
}

Ref: stg_person.id < stg_cast.actor_id

Ref: stg_person.id < stg_crew.crew_id

Table stg_genres
{
  keyid             bigint       [pk, note:'pk = id + genres id']
  id                bigint       [note:'ID phim do grouplen đặt']
  genres            varchar(500) [note:'Thể loại phim']
}

Ref: stg_movie_metadata.id < stg_genres.id

Table stg_collection
{
  keyid               bigint              [pk, note:'pk = id + collect_id']
  collect_id          bigint              [note:'TMDM ID collection']
  id                  bigint              [note:'TMDB ID của phim']
  name_               varchar(5000)       [note:'collection name']
  poster_path         varchar(5000)       [note:'Link poster collection']
  backdrop_path       varchar(5000)       [note:'Link backdrop collection']
}

Ref: stg_movie_metadata.id < stg_collection.id





