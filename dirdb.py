#! /usr/bin/env python3
#
# dirdb
# =====
#
# dirdb is a companion tool that helps to reduce unnecessary file transfers
# when a tool like rsync is used to synchronize file hierarchies.
#
# In each source and destination hierarchy, dirdb stores at least one sqlite
# database that contains information about each file like size and hash of its
# contents. With this information about each source and destination directory,
# dirdb generates a shell script that tries to synchronize the destination to
# the source directry by simple file operations like "move" or "copy".
#
# A file hierarchy can contain multiple sqlite databases in order to quickly
# synchronize only a part of the tree.
#
# To unambiguously identfy a file, the content of files in the source and
# destination hierarchy are hashed. As this can be a time expensive operation,
# dirdb can be instructed to only hash parts of the file.
#

import sys, os, sqlite3, hashlib, argparse

def hash_file(filepath, bufsize=128 * 1024):
	h = hashlib.md5()
	buffer = bytearray(bufsize)
	
	buffer_view = memoryview(buffer)
	with open(filepath, 'rb', buffering=0) as f:
		while True:
			n = f.readinto(buffer_view)
			if not n:
				break
			h.update(buffer_view[:n])
	return h.hexdigest()

def hash_file_partial(filepath, chunk_size=4096):
	fsize = os.stat(filepath).st_size
	h = hashlib.md5()
	
	with open(filepath, 'rb', buffering=0) as f:
		if fsize <= chunk_size * 2:
			h.update(f.read())
		else:
			h.update(f.read(chunk_size))
			f.seek(chunk_size, 2)
			h.update(f.read(chunk_size))
	return h.hexdigest()

def process_dbpath(path, gather_only=False, filelist_gen=None, new_files=None, dbs=None, cursors=None, all_files=[]):
	if args.verbose:
		print("processing DB in", path)
	
	path = path.rstrip("/")
	
	if os.path.isdir(path):
		dbpath = path+"/"+args.dbfilename
	else:
		dbpath = path
	
	if not os.path.isfile(dbpath):
		print("creating", dbpath)
	
	if path not in dbs:
		lite = sqlite3.connect(dbpath)
		lite.row_factory = sqlite3.Row
		dbs[path] = lite
	else:
		lite = dbs[path]
	
	if path not in cursors:
		cur = lite.cursor()
		cursors[path] = cur
	else:
		cur = cursors[path]
	
	res = cur.execute("SELECT name FROM sqlite_master")
	entries = res.fetchall()
	
	files_found = sub_dbs_found = config_found = False
	for entry in entries:
		if entry["name"] == "files":
			files_found = True
		if entry["name"] == "sub_dbs":
			sub_dbs_found = True
		if entry["name"] == "config":
			config_found = True
	if not files_found:
		cur.execute("CREATE TABLE files(filename, relpath, size, hash, parthash)")
	if not sub_dbs_found:
		cur.execute("CREATE TABLE sub_dbs(relpath)")
	if not config_found:
		cur.execute("CREATE TABLE config(key, value)")
	
	root_dir = os.path.dirname(dbpath)

	check_hash = False
	
	if filelist_gen is None:
		filegen = os.walk(root_dir)
	else:
		filegen = filelist_gen
	
	file_count = 0
	for root, dirs, files in filegen:
		for f in files:
			if f == args.dbfilename:
				continue
			
			file_count += 1
			
			fpath = root+"/"+f
			relpath = fpath[len(root_dir)+1:]
			
			if args.verbose > 1:
				if gather_only:
					print("looking for", relpath, "in db", path, end="")
				else:
					print("hashing", relpath, end="")
				if filelist_gen:
					print(" [%d/%d]" %(file_count, len(filelist_gen)))
				else:
					print("")
			
			fstat = os.stat(fpath)
			fsize = fstat.st_size
			
			res = cur.execute(f"SELECT filename, relpath, size, hash, parthash FROM files WHERE size == {fsize}")
			entries = res.fetchall()
			
			skip = False
			for entry in entries:
				if entry[1] == relpath:
					skip = True
					break
			
			if skip:
				continue
			
			if new_files is not None and (root_dir not in new_files or new_files[root_dir] != filelist_gen):
				if root_dir not in new_files:
					new_files[root_dir] = []
				new_files[root_dir].append( (root, [], [f]) )
			
			if gather_only:
				continue
			
			if config["partial_hash"]:
				fhash = hash_file_partial(fpath, config["partial_hash_size"])
				fullhash = None
				parthash = fhash
				hid = "parthash"
			else:
				fhash = hash_file(fpath)
				fullhash = fhash
				parthash = None
				hid = "hash"
			
			print("adding", relpath, "to db", path)
			
			cur.execute(f"""
				INSERT INTO files VALUES
				("{f}", "{relpath}", {fsize}, "{fullhash}", "{parthash}")
				""")

	lite.commit()
	
	res = cur.execute("SELECT relpath FROM sub_dbs")
	entries = res.fetchall()

	for entry in entries:
		if not os.path.isfile(entry[0]):
			print("removed db", entry[0])
			cur.execute(f"""
				DELETE FROM sub_dbs WHERE relpath == \"{entry[0]}\"
				""")
			
			lite.commit()
	
	res = cur.execute("SELECT filename, relpath, size, hash FROM files")
	entries = res.fetchall()

	for entry in entries:
		fpath = root_dir+"/"+entry[1]
		
		for sub_db in all_files.keys():
			if path.startswith(sub_db):
				continue
			if fpath.startswith(sub_db):
				print("will remove", entry[1], "- belongs to sub DB", sub_db)
				
				cur.execute(f"""
					DELETE FROM files WHERE relpath == \"{entry[1]}\"
					""")
				
				lite.commit()
		
		if not os.path.isfile(fpath):
			if path not in missing_files:
				missing_files[path] = []
			missing_files[path].append(entry)
			
			if args.verbose:
				print("missing file", fpath)
			
			continue
		
		fstat = os.stat(fpath)
		fsize = fstat.st_size
		
		if entry[2] != fsize:
			print("size differs", fsize, entry[2])

def open_db_tree(path):
	path = path.rstrip("/")
	
	if os.path.isdir(path):
		dbpath = path+"/"+args.dbfilename
	else:
		dbpath = path
	
	if not os.path.isfile(dbpath):
		print("no db", dbpath)
		return
	
	lite = sqlite3.connect(dbpath)
	lite.row_factory = sqlite3.Row
	dest_dbs[path] = lite
	
	cur = lite.cursor()
	dest_cursors[path] = cur

def find_dbs(path):
	dblist=[]
	for root, dirs, files in os.walk(path):
		if args.dbfilename in files:
			dblist.append(root)
			continue
	return dblist

def open_db(path):
	path = path.rstrip("/")
	
	if os.path.isdir(path):
		dbpath = path+"/"+args.dbfilename
	else:
		dbpath = path
	
	if not os.path.isfile(dbpath):
		print("creating", dbpath)
	
	if path not in dbs:
		db = sqlite3.connect(dbpath)
		db.row_factory = sqlite3.Row
	else:
		db = dbs[path]
	
	if path not in cursors:
		cursor = db.cursor()
	else:
		cursor = cursors[path]
	
	return db, cursor

def prepare_path(path):
	path = path.rstrip("/")
	path = os.path.expanduser(path)
	return path

def update_paths(paths):
	dbpaths = [prepare_path(arg) for arg in paths]
	
	if args.verbose > 1:
		print("gathering all files", dbpaths)
	all_files = {}
	file_count = 0
	def find_files(dbpath):
		nonlocal file_count
		for root, dirs, files in os.walk(dbpath):
			if root != dbpath and args.dbfilename in files:
				dirs[:] = []
				find_files(root)
				continue
			for f in files:
				if root+"/"+f == os.getcwd()+"/"+args.scriptname:
					continue
				
				if dbpath not in all_files:
					all_files[dbpath] = []
				
				all_files[dbpath].append( (root, [], [f]) )
				file_count += 1
	for dbpath in dbpaths:
		find_files(dbpath)
	if args.verbose > 0:
		print(file_count, "files in", list(all_files.keys()))

	new_files = {}
	if args.verbose > 1:
		print("find new files")
	if all_files:
		for dbpath in all_files:
			process_dbpath(dbpath, filelist_gen=all_files[dbpath], gather_only=True, new_files=new_files, dbs=dbs, cursors=cursors, all_files=all_files)
	else:
		for dbpath in dbpaths:
			process_dbpath(dbpath, gather_only=True, new_files=new_files, dbs=dbs, cursors=cursors, all_files=all_files)
	new_file_count = 0
	for dbpath in new_files:
		new_file_count += len(new_files[dbpath])
	if args.verbose > 0:
		print(new_file_count, "new files")

	if args.verbose > 1:
		print("hashing new files")
	if new_files:
		for dbpath in new_files:
			process_dbpath(dbpath, filelist_gen=new_files[dbpath], dbs=dbs, cursors=cursors, all_files=all_files)
	
	for dbpath in missing_files:
		for entry in missing_files[dbpath]:
			found = False
			
			for i_dbpath in cursors:
				res = cursors[i_dbpath].execute(f"""SELECT * FROM files WHERE size == {entry["size"]}""")
				i_entries = res.fetchall()
				print("moved", dbpath+"/"+entry["relpath"], "to", [ i_entry["relpath"] for i_entry in i_entries ])
				found = True
			
			if not found:
				print("removed", entry["relpath"])
			
			cursors[dbpath].execute(f"""
				DELETE FROM files WHERE relpath == \"{entry[1]}\"
				""")
	
	if args.list_dups:
		dup_hashes = []
		for cur in cursors.values():
			res = cur.execute("SELECT filename, relpath, size, hash, parthash FROM files")
			entries = res.fetchall()
			
			for entry in entries:
				if entry["hash"] in dup_hashes:
					continue
				
				dups = []
				for sub_cur in cursors.values():
					res = sub_cur.execute(f"SELECT filename, relpath, size, hash FROM files WHERE hash == '{entry[3]}'")
					sub_entries = res.fetchall()
					for sub_entry in sub_entries:
						dups.append(sub_entry["relpath"])
				
				if len(dups) > 1:
					dup_hashes.append(entry["hash"])
					print(dups)

	for db in dbs.values():
		db.commit()

def gen_sync_script(sources, dests):
	dest_dbs = {}
	dest_cursors = {}
	
	f = open(args.scriptname, "w")
	f.write("#! /bin/sh -e\n\n")
	
	src_dbs = {}
	src_dbpaths = []
	for path in sources:
		path = prepare_path(path)
		src_dbpaths.extend( find_dbs(path) )
	
	for path in src_dbpaths:
		db, cursor = open_db(path)
		src_dbs[path] = cursor
	
	dst_dbs = {}
	dst_dbpaths = []
	for path in dests:
		path = prepare_path(path)
		dst_dbpaths.extend( find_dbs(path) )
	
	for path in dst_dbpaths:
		db, cursor = open_db(path)
		dst_dbs[path] = cursor
	
	mkdirs = []
	transfer_bytes = 0
	n_actions = 0
	subdir = None
	done = []
	for dbpath in src_dbs:
		res = src_dbs[dbpath].execute(f"""SELECT * FROM files WHERE size > 0""")
		entries = res.fetchall()
		
		if subdir is not None:
			f.write("\ncd \"${OLDPWD}\" # from "+subdir+"\n")
		f.write("OLDPWD=\"$(pwd)\"; cd \""+dbpath+"\"\n\n")
		subdir=dbpath
		
		i = -1
		for entry in entries:
			i += 1
			
			print(f"\r{i}/{len(entries)} ", end="")
			
			if entry["parthash"] in done:
				continue
			
			found = False
			
			l_num = 0
			sql_dup_entries = {}
			for i_dbpath in src_dbs:
				res = src_dbs[i_dbpath].execute(f"""SELECT * FROM files WHERE size == {entry["size"]} AND parthash == '{entry["parthash"]}'""")
				sql_dup_entries[i_dbpath] = res.fetchall()
				l_num += len(sql_dup_entries[i_dbpath])
			
			r_num = 0
			sql_rem_dup_entries = {}
			for i_dbpath in dst_dbs:
				res = dst_dbs[i_dbpath].execute(f"""SELECT * FROM files WHERE size == {entry["size"]} AND parthash == '{entry["parthash"]}'""")
				sql_rem_dup_entries[i_dbpath] = res.fetchall()
				r_num += len(sql_rem_dup_entries[i_dbpath])
			
			done.append(entry["parthash"])
			
			if l_num == 1 and r_num == 1:
				src = sql_dup_entries[list(sql_dup_entries.keys())[0]][0]
				dst = sql_rem_dup_entries[list(sql_rem_dup_entries.keys())[0]][0]
				
				if src["relpath"] != dst["relpath"]:
					mkdir = os.path.dirname(src["relpath"])
					if not mkdir:
						continue
					if mkdir not in mkdirs:
						mkdirs.append(mkdir)
						f.write(f"""mkdir ${{MKDIRFLAGS}} -p \"{mkdir}\"\n""")
					
					f.write(f"""mv ${{MVFLAGS}} \"{dst["relpath"]}\" \"{src["relpath"]}\"\n""")
					n_actions += 1
				
				continue
			if l_num == 0:
				import pdb; pdb.set_trace()
			if r_num == 0:
				f.write(f"""# missing on destination: \"{entry["relpath"]}\"\n""")
				transfer_bytes += entry["size"]
				continue
			
			rem_dup_entries = {}
			for r_dbpath in sql_rem_dup_entries:
				rem_dup_entries[r_dbpath] = []
				for r_entry in sql_rem_dup_entries[r_dbpath]:
					e = dict()
					e.update(r_entry)
					
					rem_dup_entries[r_dbpath].append(e)
			
			dup_entries = {}
			for l_dbpath in sql_dup_entries:
				dup_entries[l_dbpath] = []
				for l_entry in sql_dup_entries[l_dbpath]:
					e = dict()
					e.update(l_entry)
					
					dup_entries[l_dbpath].append(e)
			
			for l_dbpath in dup_entries:
				for l_entry in dup_entries[l_dbpath]:
					for r_dbpath in rem_dup_entries:
						for r_entry in rem_dup_entries[r_dbpath]:
							if l_entry["relpath"] == r_entry["relpath"]:
								l_entry["matched"] = True
								r_entry["matched"] = True
								
								if l_dbpath == dbpath and l_entry["relpath"] == entry["relpath"]:
									found = True
								break
						
						if l_entry.get("matched", False):
							break
			
			for l_dbpath in dup_entries:
				for l_entry in dup_entries[l_dbpath]:
					if l_entry.get("matched", False):
						continue
					
					for r_dbpath in rem_dup_entries:
						for r_entry in rem_dup_entries[r_dbpath]:
							# if getattr(r_entry, "matched", False):
							if r_entry.get("matched", False):
								continue
							
							mkdir = os.path.dirname(l_entry["relpath"])
							if mkdir not in mkdirs:
								mkdirs.append(mkdir)
								f.write(f"""mkdir ${{MKDIRFLAGS}} -p \"{mkdir}\"\n""")
							
							f.write(f"""mv ${{MVFLAGS}} \"{r_entry["relpath"]}\" \"{l_entry["relpath"]}\"\n""")
							n_actions += 1
							l_entry["matched"] = True
							r_entry["matched"] = True
							
							if l_dbpath == dbpath and l_entry["relpath"] == entry["relpath"]:
								found = True
							
							break
						
						if l_entry.get("matched", False):
							break
					
					if not l_entry.get("matched", False):
						for r_dbpath in rem_dup_entries:
							for r_entry in rem_dup_entries[r_dbpath]:
								mkdir = os.path.dirname(l_entry["relpath"])
								if mkdir not in mkdirs:
									mkdirs.append(mkdir)
									f.write(f"""mkdir ${{MKDIRFLAGS}} -p \"{mkdir}\"\n""")
								
								f.write(f"""cp ${{CPFLAGS}} --reflink \"{r_entry["relpath"]}\" \"{l_entry["relpath"]}\"\n""")
								n_actions += 1
								l_entry["matched"] = True
								
								if l_dbpath == dbpath and l_entry["relpath"] == entry["relpath"]:
									found = True
								
								break
							
							if l_entry.get("matched", False):
								break
					
					if not l_entry.get("matched", False) and r_num > 0:
						print("unmatched", l_entry)
						import pdb; pdb.set_trace()
			
			for r_dbpath in rem_dup_entries:
				for r_entry in rem_dup_entries[r_dbpath]:
					if r_entry.get("matched", False):
						continue
					
					print("unmatched r", r_entry)
					import pdb; pdb.set_trace()
			
			for l_dbpath in dup_entries:
				if not l_entry.get("matched", False) and r_num > 0:
					print("unmatched l", l_entry)
					import pdb; pdb.set_trace()
			
			if not found and r_num > 0:
				import pdb; pdb.set_trace()
	
	if subdir is not None:
		f.write("cd \"${OLDPWD}\" # from "+subdir+"\n")
	
	print("\nn_actions", n_actions)
	
	display_value = 0
	unit = None
	units = ["B", "KB", "MB", "GB", "TB"]
	for u in range(len(units)):
		if transfer_bytes < pow(1000, (u+1)):
			break
	display_value = int(transfer_bytes / pow(1000, u))
	
	print("\nstill to transfer:", display_value, units[u])

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("-v", "--verbose", action="count", default=0)
	parser.add_argument("--dbfilename", default=".dir.db", help="the filename of the db (default: .dir.db)")
	parser.add_argument("--scriptname", default="update.sh", help="the script filename (default: update.sh)")
	parser.add_argument("--list-dups", action="store_true", help="list duplicate files")
	parser.add_argument("-P", "--partial-hash", action="store_true", default=True, help="only hash a number of bytes from the beginning and end of a file")
	parser.add_argument("--partial-hash-size", type=int, default=4096, help="size of bytes that are used to create a hash of a file (default: 4096)")
	parser.add_argument("-g", "--gen-sync-script", action="store_true", help="create shell script that executes commands necessary to sync source and destination directories")
	parser.add_argument("-s", "--source", action="append", help="one or more source directories")
	parser.add_argument("-d", "--destination", action="append", help="one or more destination directories")
	parser.add_argument("-u", "--update", action="append", help="update all databases under this directory (can be passed multiple times)")

	args = parser.parse_args()

	missing_files = {}
	dbs = {}
	new_files = None
	cursors = {}
	config = {}

	config["partial_hash"] = args.partial_hash
	config["partial_hash_size"] = args.partial_hash_size
	
	if not args.update and not args.gen_sync_script:
		if not args.destination:
			if args.source:
				args.update = args.source
			else:
				args.update = [os.getcwd()]
		else:
			args.gen_sync_script = True
			if not args.source:
				args.source = [os.getcwd()]
	
	if args.update:
		update_paths(args.update)

	if args.gen_sync_script:
		if not args.source and args.destination:
			sources = [os.getcwd()]
		else:
			sources = args.source
		if args.source and not args.destination:
			destinations = [os.getcwd()]
		else:
			destinations = args.destination
		
		gen_sync_script(sources, destinations)
