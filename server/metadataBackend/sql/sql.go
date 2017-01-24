/**

    Plik upload server

The MIT License (MIT)

Copyright (c) <2015>
	- Mathieu Bodjikian <mathieu@bodjikian.fr>
	- Charles-Antoine Mathieu <skatkatt@root.gg>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
**/

package sql

import (
	"github.com/root-gg/juliet"
	"github.com/root-gg/plik/server/common"

	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/root-gg/utils"
	"log"
	"time"
)

// MetadataBackend object
type MetadataBackend struct {
	Config *MetadataBackendConfig

	db *gorm.DB
}

// SQLFile is an override of a common.File,
// to add an uploadID field in the object to
// be able to store it on SQL server, and link
// files to uploads since it's two different tables
type SQLFile struct {
	*common.File
	UploadID string `gorm:"column:uploadId"`
}

// SQLToken is an override of a common.Token,
// to add an userId field in the object to
// be able to store it on SQL server, and link
// tokens to users since it's two different tables
type SQLToken struct {
	*common.Token
	UserID string `gorm:"column:userId"`
}

// NewSQLMetadataBackend instantiate a new File Metadata Backend
// from configuration passed as argument
func NewSQLMetadataBackend(config map[string]interface{}) (smb *MetadataBackend) {
	smb = new(MetadataBackend)
	smb.Config = NewSQLMetadataBackendConfig(config)

	// Format DSN using information of
	// config object
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", smb.Config.Username, smb.Config.Password, smb.Config.Host, smb.Config.Port, smb.Config.Database)

	// Open connection
	c, err := gorm.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Fail to connect to SQL server : %s", err)
	}

	log.Printf("Connected to SQL server on %s", dsn)

	// Store connection in metadata backend
	if common.Config.LogLevel == "DEBUG" {
		smb.db = c.Debug()
	} else {
		smb.db = c
	}

	return
}

// Create implementation for SQL Metadata Backend
func (smb *MetadataBackend) Create(ctx *juliet.Context, upload *common.Upload) (err error) {
	log := common.GetLogger(ctx)

	if upload == nil {
		err = log.EWarning("Unable to save upload : Missing upload")
		return
	}

	// Begin transaction
	tx := smb.db.Begin()

	// Create upload
	err = tx.Create(upload).Error
	if err != nil {
		smb.db.Rollback()
		log.EWarningf("Unable to save upload : %", err)
		return err
	}

	// Create files
	for _, file := range upload.Files {
		sqlFile := new(SQLFile)
		sqlFile.File = file
		sqlFile.UploadID = upload.ID
		err = tx.Table("files").Create(sqlFile).Error
		if err != nil {
			smb.db.Rollback()
			log.EWarningf("Unable to save upload file : %", err)
			return err
		}
	}

	tx.Commit()
	return
}

// Get implementation for SQL Metadata Backend
func (smb *MetadataBackend) Get(ctx *juliet.Context, id string) (upload *common.Upload, err error) {
	log := common.GetLogger(ctx)

	if id == "" {
		err = log.EWarning("Unable to get upload : Missing upload id")
		return
	}

	// Get upload
	upload = common.NewUpload()
	err = smb.db.First(upload, "id = ?", id).Error
	if err != nil {
		return
	}

	// Get upload files
	files := make([]*common.File, 0)

	err = smb.db.Find(&files, "uploadId = ?", id).Error
	if err != nil {
		log.EWarningf("Unable to get upload files : %", err)
		return
	}

	for _, file := range files {
		upload.Files[file.ID] = file
	}

	utils.Dump(upload)

	return
}

// AddOrUpdateFile implementation for SQL Metadata Backend
func (smb *MetadataBackend) AddOrUpdateFile(ctx *juliet.Context, upload *common.Upload, file *common.File) (err error) {
	log := common.GetLogger(ctx)

	if upload == nil {
		err = log.EWarning("Unable to add file : Missing upload")
		return
	}

	if file == nil {
		err = log.EWarning("Unable to add file : Missing file")
		return
	}

	// Add or update file in uplaod
	sqlFile := new(SQLFile)
	sqlFile.File = file
	sqlFile.UploadID = upload.ID

	err = smb.db.Table("files").Save(sqlFile).Error
	if err != nil {
		log.EWarningf("Unable to update file in upload : %", err)
		return
	}

	return
}

// RemoveFile implementation for SQL Metadata Backend
func (smb *MetadataBackend) RemoveFile(ctx *juliet.Context, upload *common.Upload, file *common.File) (err error) {
	log := common.GetLogger(ctx)

	if upload == nil {
		err = log.EWarning("Unable to remove file : Missing upload")
		return
	}

	if file == nil {
		err = log.EWarning("Unable to remove file : Missing file")
		return
	}

	return nil
}

// Remove implementation for SQL Metadata Backend
func (smb *MetadataBackend) Remove(ctx *juliet.Context, upload *common.Upload) (err error) {
	log := common.GetLogger(ctx)

	if upload == nil {
		err = log.EWarning("Unable to remove upload : Missing upload")
		return
	}

	// Delete files of upload
	err = smb.db.Table("files").Delete(common.File{}, "uploadId = ?", upload.ID).Error
	if err != nil {
		err = log.EWarningf("Unable to delete upload files : %s", err)
		return
	}

	// Delete upload
	err = smb.db.Table("uploads").Delete(upload).Error
	if err != nil {
		err = log.EWarningf("Unable to delete upload : %s", err)
		return
	}

	return
}

// GetUploadsToRemove implementation for File Metadata Backend
func (smb *MetadataBackend) GetUploadsToRemove(ctx *juliet.Context) (ids []string, err error) {
	log := common.GetLogger(ctx)

	// Init ids list
	ids = make([]string, 0)

	// Look for expired uploads
	err = smb.db.Table("uploads").Where(" ttl > ? AND ? > uploadDate+ttl", 0, int(time.Now().Unix())).Pluck("id", &ids).Error
	if err != nil {
		err = log.EWarningf("Unable to get uploads to remove : %s", err)
		return ids, err
	}

	return ids, nil
}

// SaveUser implementation for File Metadata Backend
func (smb *MetadataBackend) SaveUser(ctx *juliet.Context, user *common.User) (err error) {
	log := common.GetLogger(ctx)

	// Begin transaction
	tx := smb.db.Begin()

	// Save user
	err = smb.db.Table("users").Save(user).Error
	if err != nil {
		tx.Rollback()
		log.EWarningf("Fail to create user : %s", err)
		return
	}

	// Save user's tokens
	for _, token := range user.Tokens {
		sqlToken := new(SQLToken)
		sqlToken.Token = token
		sqlToken.UserID = user.ID

		err = tx.Table("usersTokens").Assign(sqlToken).FirstOrCreate(&sqlToken).Error
		if err != nil {
			tx.Rollback()
			log.EWarningf("Fail to create user token : %s", err)
			return err
		}

	}

	tx.Commit()
	return
}

// GetUser implementation for File Metadata Backend
func (smb *MetadataBackend) GetUser(ctx *juliet.Context, id string, token string) (user *common.User, err error) {
	log := common.GetLogger(ctx)

	if id == "" && token == "" {
		err = log.EWarning("Unable to get user : Missing user id or token")
		return
	}

	user = &common.User{}
	if id != "" {

		// Get user
		err = smb.db.Table("users").Find(user, "id = ?", id).Error
		if err != nil {
			if err != gorm.ErrRecordNotFound {
				return nil, err
			}
		}

		// Get user tokens
		err = smb.db.Table("usersTokens").Find(&user.Tokens, "userId = ?", user.ID).Error
		if err != nil {
			if err != gorm.ErrRecordNotFound {
				return nil, err
			}
		}

	} else if token != "" {

		var userId int
		err = smb.db.Table("usersTokens").Find(user.Tokens, "token = ?", token).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil, nil
			} else {
				return nil, err
			}
		}

		err = smb.db.Table("users").Find(user, "id = ?", userId).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil, nil
			} else {
				return nil, err
			}
		}
	}

	return
}

// RemoveUser implementation for File Metadata Backend
func (smb *MetadataBackend) RemoveUser(ctx *juliet.Context, user *common.User) (err error) {
	log := common.GetLogger(ctx)

	// Remove user tokens
	err = smb.db.Table("usersTokens").Delete(nil, " userId = ?", user.ID).Error
	if err != nil {
		log.EWarningf("Fail to delete user tokens : %s", err)
		return
	}

	// Remove user
	err = smb.db.Table("users").Delete(user).Error
	if err != nil {
		log.EWarningf("Fail to delete user : %s", err)
		return
	}

	return
}

// GetUserUploads implementation for File Metadata Backend
func (smb *MetadataBackend) GetUserUploads(ctx *juliet.Context, user *common.User, token *common.Token) (ids []string, err error) {
	log := common.GetLogger(ctx)

	if user == nil {
		err = log.EWarning("Unable to get user uploads : Missing user")
		return
	}

	if token != nil {
		err = smb.db.Table("uploads").Where(" user = ? AND token = ?", user.ID, token.Token).Pluck("id", &ids).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return ids, nil
			} else {
				return ids, err
			}
		}

	} else {
		err = smb.db.Table("uploads").Where(" user = ?", user.ID).Pluck("id", &ids).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return ids, nil
			} else {
				return ids, err
			}
		}
	}

	return
}
