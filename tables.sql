

CREATE TABLE `uploads` (

    id                      varchar(255)    NOT NULL PRIMARY KEY,
    uploadDate              INT             NOT NULL,
    ttl                     INT             NOT NULL DEFAULT 0,
    downloadDomain          varchar(255)    NULL,
    uploadIp                varchar(255)    NULL,
    comments                text            NULL DEFAULT NULL,
    uploadToken             varchar(255)    NOT NULL,
    user                    varchar(255)    NULL DEFAULT NULL,
    token                   varchar(255)    NULL DEFAULT NULL,
    admin                   tinyint(1)      NOT NULL DEFAULT 0,
    stream                  tinyint(1)      NOT NULL DEFAULT 0,
    oneShot                 tinyint(1)      NOT NULL DEFAULT 0,
    removable               tinyint(1)      NOT NULL DEFAULT 0,
    protectedByPassword     tinyint(1)      NOT NULL DEFAULT 0,
    login                   varchar(255)    NULL DEFAULT NULL,
    password                varchar(255)    NULL DEFAULT NULL,
    protectedByYubikey      tinyint(1)      NOT NULL DEFAULT 0,
    yubikey                 varchar(20)     NULL DEFAULT NULL,

    INDEX `uploadToken`     (`uploadToken`),
    INDEX `user`            (`user`)

) ENGINE=InnoDB CHECKSUM=1 DEFAULT CHARSET=utf8;


CREATE TABLE `files` (

    id                      varchar(255)    NOT NULL PRIMARY KEY,
    uploadId                varchar(255)    NOT NULL,

    fileName                varchar(255)    NOT NULL,
    fileMd5                 varchar(255)    NOT NULL,
    status                  varchar(255)    NOT NULL,
    fileType                varchar(255)    NOT NULL,
    fileUploadDate          INT             NOT NULL,
    fileSize                INT             NOT NULL DEFAULT 0,
    backendDetails          TEXT            NULL,
    reference               varchar(255)    NULL,

    INDEX `uploadId` (`uploadId`)

) ENGINE=InnoDB CHECKSUM=1 DEFAULT CHARSET=utf8;


CREATE TABLE `users` (

    id                      varchar(255)    NOT NULL PRIMARY KEY,
    login                   varchar(255)    NOT NULL,
    name                    varchar(255)    NOT NULL,
    email                   varchar(255)    NULL

) ENGINE=InnoDB CHECKSUM=1 DEFAULT CHARSET=utf8;

CREATE TABLE `usersTokens` (

    token                   varchar(255)    NOT NULL PRIMARY KEY,
    userId                  varchar(255)    NOT NULL,
    creationDate            INT             NOT NULL,
    comment                 TEXT            NULL

) ENGINE=InnoDB CHECKSUM=1 DEFAULT CHARSET=utf8;

