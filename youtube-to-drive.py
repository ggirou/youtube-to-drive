#!/usr/bin/python

from __future__ import unicode_literals, print_function
import os, sys, re, datetime, io, csv, youtube_dl, unicodedata
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from tendo import singleton

def readCsv(gFileId):
  file = drive.CreateFile({'id': gFileId})
  content = file.GetContentString(mimetype='text/csv')
  content = unicode(unicodedata.normalize('NFKD', content).encode('ascii','ignore'))
  with io.StringIO(content) as csvfile:
    return [row for row in csv.DictReader(csvfile)]


def dowloadedYoutubeIds(gFolderId):
  files = drive.ListFile(
    {'q': "'%s' in parents and trashed=false" % gFolderId}).GetList()
  id_extracts = [re.search(r'-(.{11})\.mp4$', f['title']) for f in files]
  # print(files, file=sys.stderr)
  return set([extract.group(1) for extract in id_extracts if extract] + [f['description'] for f in files if 'description' in f])


class MyLogger(object):
  def debug(self, msg):
    print(msg, file=sys.stderr)

  def warning(self, msg):
    print(msg, file=sys.stderr)

  def error(self, msg):
    print(msg)


class Downloader:
  def __init__(self, urls, folder_id):
    self.urls = urls.splitlines()
    self.folder_id = folder_id
    self.downloaded_files = dowloadedYoutubeIds(folder_id)
    print("urls: %s,\n folder_id: %s,\n downloaded_files: %s" % (self.urls, self.folder_id, self.downloaded_files), file=sys.stderr)

  def match_filter(self, info_dict):
    if info_dict['id'] in self.downloaded_files:
      return 'Already downloaded: %s-%s.mp4, skipping...' % (info_dict['title'], info_dict['id'])
    else:
      self.current_id = info_dict['id']
      print('%s - Downloading: %s-%s.mp4' %
          (datetime.datetime.now(), info_dict['title'], info_dict['id']))
    return None

  def on_download(self, i):
    if i['status'] == 'finished':
      file = drive.CreateFile()
      file['description'] = self.current_id
      file['parents'] = [{'id': self.folder_id}]
      file.SetContentFile(i['filename'])
      file.Upload()
      os.remove(i['filename'])
      print('%s - Downloaded: %s (%s in %s)' % (datetime.datetime.now(), i['filename'], i['_total_bytes_str'], i['_elapsed_str']))
      # print(i, file=sys.stderr)

  def download(self):
    ydl_opts = {
      'progress_hooks': [self.on_download],
      'match_filter': self.match_filter,
      'logger': MyLogger(),
      'ignoreerrors': True
    }
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
      ydl.download(self.urls)


############################################


if len(sys.argv) <= 1:
  print("Usage: %s googleDriveCsvFileId" % sys.argv[0], file=sys.stderr)
  exit(1)

csvFileId = sys.argv[1]

me = singleton.SingleInstance(flavor_id=csvFileId) # will sys.exit(-1) if other instance is running

gauth = GoogleAuth()
gauth.CommandLineAuth()
drive = GoogleDrive(gauth)

os.chdir('/tmp')

rows = readCsv(csvFileId)
for row in rows:
  Downloader(row['Urls'], row['Folder']).download()
