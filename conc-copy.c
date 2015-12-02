#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdbool.h>
#include <errno.h>
#include <pthread.h>
#include <dirent.h>
#include <string.h>

#define MAX_CHUNK (256 * 1024 * 1024)
#define BS_SIZE (1024 * 1024)
#define MAX_THREADS 100
#define MAX_FILES 75000

#define HASH_BUCKETS_NUM 311u

extern int errno;

int open_files[MAX_FILES] = {0};

int dec_file_ref(int fd) {
	return __atomic_sub_fetch(&open_files[fd], 1, __ATOMIC_SEQ_CST);
}

struct hash_node {
	dev_t st_dev;
	ino_t st_ino;
	struct hash_node *next;
};

int is_hash_node_equals(struct hash_node *node1, struct hash_node *node2) {
	return (node1->st_dev == node2->st_dev && node1->st_ino == node2->st_ino);
}

struct hash_node *hash_table[HASH_BUCKETS_NUM];

void add_to_hash_table(struct stat *st) {
	int key = st->st_ino % HASH_BUCKETS_NUM;
	struct hash_node *p = hash_table[key];
	struct hash_node *cur_node = (struct hash_node*)malloc(sizeof(struct hash_node));
	cur_node->st_dev = st->st_dev;
	cur_node->st_ino = st->st_ino;
	cur_node->next = NULL;
	if (p == NULL) {
		hash_table[key] = cur_node;
		return;
	}
	while (p) {
		if (is_hash_node_equals(cur_node, p)) {
			return;
		}
		if (p->next == NULL) {
			p->next = cur_node;
		}
		p = p->next;
	}
}

bool is_in_hash_table(struct stat *st) {
	int key = st->st_ino % HASH_BUCKETS_NUM;
	struct hash_node *p = hash_table[key];
	while (p) {
		if (p->st_dev == st->st_dev && p->st_ino == st->st_ino) {
			return true;
		}
		p = p->next;
	}
	return false;
}

enum {
	FORCE_COPY_FLAG = 1 << 0, // -f
	RECURSIVE_COPY_FLAG = 1 << 1, // -r
};

struct copy_chunk {
	int src_fd;
	int dest_fd;
	off_t beg_pos;
	off_t offset;
};

struct item_node {
	struct copy_chunk *cur_chunk;
	struct item_node *next;
};

struct workqueue {
	pthread_spinlock_t s_lock;
	struct item_node *head;
	struct item_node *tail;
};

struct item_node *get_item(struct workqueue *wq) {
	pthread_spin_lock(&wq->s_lock);
	struct item_node *cur_node = wq->head->next;
	wq->head->next = cur_node->next;
	if (wq->tail == cur_node) {
		wq->tail = wq->head;
	}
	pthread_spin_unlock(&wq->s_lock);
	cur_node->next = NULL;
	return cur_node;
}

void add_item(struct item_node *node, struct workqueue *wq) {
	pthread_spin_lock(&wq->s_lock);
	node->next = wq->tail->next;
	wq->tail->next = node;
	wq->tail = node;
	pthread_spin_unlock(&wq->s_lock);
}

bool is_wq_empty(struct workqueue *wq) {
	pthread_spin_lock(&wq->s_lock);
	bool is_empty = (wq->head == wq->tail) ? true : false;
	pthread_spin_unlock(&wq->s_lock);
	return is_empty;
}

int split_file(int src_fd, int dest_fd, struct workqueue *queue, off_t fsize) {
	int chunk_count = (fsize -1) / MAX_CHUNK + 1;
	int i = 0;
	off_t cur_off = 0;
	off_t beg_pos = 0;
	open_files[src_fd] = 0;
	open_files[dest_fd] = 0;
	for (i = 0; i < chunk_count; ++i) {
		cur_off += MAX_CHUNK;
		struct item_node *pnode = (struct item_node*)malloc(sizeof(struct item_node));
		if (!pnode) {
			printf("malloc item_node failed!\n");
			return -1;
		}
		pnode->cur_chunk = (struct copy_chunk*)malloc(sizeof(struct copy_chunk));
		if (!pnode->cur_chunk) {
			printf("malloc copy_chunk failed!\n");
			return -1;
		}
		pnode->cur_chunk->src_fd = src_fd;
		pnode->cur_chunk->dest_fd = dest_fd;
		pnode->cur_chunk->beg_pos = beg_pos;
		pnode->cur_chunk->offset = cur_off < fsize ? cur_off : fsize;
		open_files[src_fd]++;
		open_files[dest_fd]++;
		pnode->next = NULL;
		add_item(pnode, queue);
		beg_pos += MAX_CHUNK;
	}
	return chunk_count;
}

long long copy_item(struct item_node *cur_node) {
	struct copy_chunk *cur_chunk = cur_node->cur_chunk;
	char *buf = (char*)malloc(sizeof(char) * BS_SIZE);
	off_t remain_size = cur_chunk->offset - cur_chunk->beg_pos;
	off_t cur_offset = cur_chunk->beg_pos;
	long long total = 0;
	while (remain_size > 0) {
		long long rd_count = pread(cur_chunk->src_fd, buf, BS_SIZE, cur_offset);
		if (rd_count < 0) {
			printf("pread error, errno is: %d", errno);
			return -1;
		}
		long long wr_count = pwrite(cur_chunk->dest_fd, buf, rd_count, cur_offset);
		if (wr_count < 0) {
			printf("pwrite error, errno is: %d", errno);
			return -1;
		}
		if (wr_count < rd_count) {
			printf("unexpected occurs in pwrite process!\n");
			return -1;
		}
		total += rd_count;
		cur_offset += rd_count;
		remain_size -= rd_count;
	}
	free(buf);
	return total;

}

void *copy_task(void *work_queue) {
	struct workqueue *wq = (struct workqueue*)work_queue;
	while (!is_wq_empty(wq)) {
		struct item_node *cur_node = get_item(wq);
		long cret = copy_item(cur_node);
		if (cret < 0) {
			printf("Error occurs in copy thread: %ld\n", pthread_self());
			pthread_exit((void*)cret);
		}

		if (dec_file_ref(cur_node->cur_chunk->src_fd) == 0) {
			close(cur_node->cur_chunk->src_fd);
		}
		if (dec_file_ref(cur_node->cur_chunk->dest_fd) == 0) {
			close(cur_node->cur_chunk->dest_fd);
		}
		free(cur_node->cur_chunk);
		free(cur_node);
	}
}

char *last_char_is(const char *s, int c)
{
	if (s && *s) {
		size_t sz = strlen(s) - 1;
		s += sz;
		if ( (unsigned char)*s == c)
			return (char*)s;
	}
	return NULL;
}

char *get_last_component(char* path) {
	char *p = path + strlen(path) - 1;
	while (*p == '/' && p != path) {
		*p = '\0';
		--p;
	}
	char *rslash = strrchr(path, '/');
	if (rslash == NULL) {
		return path;
	}
	if (rslash == path) {
		return path;
	}
	return rslash + 1;
}

char *concat_path_file(const char *path, const char *filename)
{
	char *lc;
	if (!path)
		path = "";
	lc = last_char_is(path, '/');
	while (*filename == '/')
		filename++;
	char *full_path;
	if (asprintf(&full_path, "%s%s%s", path, (lc==NULL ? "/" : ""), filename) < 0) {
		printf("concat_path_file failed in asprintf\n");
		return NULL;
	}
	return full_path;
}


char *concat_subpath_file(const char *path, const char *f)
{
	if (f && (*f == '.' && ((*(f+1) == '\0') || (*(f+1) == '.' && (*(f+2) == '\0'))))) {
		return NULL;
	}
	return concat_path_file(path, f);
}

int handle_reg_file(const char *source, const char *dest, mode_t new_mode, off_t fsize, struct workqueue *p_wq) {
	int src_fd = -1;
	int dest_fd = -1;
	int chunk_count = -1;
	if ((dest_fd = open(dest, O_WRONLY|O_CREAT|O_TRUNC, new_mode)) < 0) {
		printf("open '%s' failed!\n", dest);
		return -1;
	}
	if ((src_fd = open(source, O_RDONLY)) < 0) {
		printf("open '%s' failed!\n", source);
		return -1;
	}
	chunk_count = split_file(src_fd, dest_fd, p_wq, fsize);
	if(chunk_count < 0) {
		printf("split file '%s' failed\n", source);
		return -1;
	}
	return chunk_count;
}

int generate_tasks(const char *source, const char *dest, int copy_flags, struct workqueue *p_wq) {
	struct stat source_stat;
	struct stat dest_stat;
	int retval = 0;
	int does_exists = 0;
	int chunk_count = 0;
	int force_ovr = (copy_flags & FORCE_COPY_FLAG) ? 1 : 0;
	if ((lstat(source, &source_stat)) < 0) {
		printf("cant't stat '%s', maybe it does not exists\n", source);
		return -1;
	}
	if (lstat(dest, &dest_stat) < 0) {
		if (errno != ENOENT) {
			printf("cant't stat '%s', something unexpected", dest);
			return -1;
		}
	} else {
		if (source_stat.st_dev == dest_stat.st_dev
				&& source_stat.st_ino == dest_stat.st_ino) {
			printf("'%s' and '%s' are the same file\n", source, dest);
			return -1;
		}
		does_exists = 1;
	}
	if (S_ISDIR(source_stat.st_mode)) {  //if copy a directory
		if (is_in_hash_table(&source_stat)) { // the source has been created before
			printf("can't copy directory '%s' into itself '%s', " 
					"it can lead cycle copy\n", source, dest);
			return -1;
		}
		mode_t saved_umask = 0;
		if (!(copy_flags & RECURSIVE_COPY_FLAG)) {
			printf("copy directory '%s' without -r option\n", source);
			return -1;	
		}
		if (does_exists) {
			if (!S_ISDIR(dest_stat.st_mode)) {
				printf("target '%s' is not a directory\n", dest);
				return -1;
			}
		} else {
			mode_t mode;
			saved_umask = umask(0);
			mode = source_stat.st_mode;
			mode |= S_IRWXU;
			if (mkdir(dest, mode) < 0) {
				umask(saved_umask);
				printf("can't create directory '%s'\n", dest);
				return -1;
			}
			umask(saved_umask);
			if (lstat(dest, &dest_stat) < 0) {
				printf("can't stat directory '%s'\n", dest);
				return -1;
			}
			add_to_hash_table(&dest_stat);
		}
		if (lstat(dest, &dest_stat) < 0) {
			printf("can't stat '%s'", dest);
			return -1;
		}
		DIR *dp;
		if ((dp = opendir(source)) == NULL) {
			printf("can't open source dir '%s'\n", source);
			return -1;
		}
		struct dirent *d;
		while ((d = readdir(dp)) != NULL) {
			char *new_source, *new_dest;
			new_source = concat_subpath_file(source, d->d_name);
			if (new_source == NULL) {
				continue;
			}
			new_dest = concat_path_file(dest, d->d_name);
			if (generate_tasks(new_source, new_dest, copy_flags, p_wq) < 0) {
				return -1;
			}
			free(new_source);
			free(new_dest);
		}
		closedir(dp);
		if (!does_exists && chmod(dest, source_stat.st_mode & ~saved_umask) < 0) {
			printf("can't preserve %s of '%s'", "permissions\n", dest);
			return -1;
		}
		return 0;

	} 
	if (S_ISREG(source_stat.st_mode) || 
			(S_ISLNK(source_stat.st_mode) && !(copy_flags & RECURSIVE_COPY_FLAG))) {
		if (does_exists && !force_ovr) {
			printf("'%s' already exists, you can do it use -f option\n", dest);
			return -1;
		} else {
			mode_t new_mode = source_stat.st_mode;
			if (handle_reg_file(source, dest, new_mode, source_stat.st_size, p_wq) < 0) {
				printf("copy '%s' to '%s' failed!\n", source, dest);
				return -1;
			}
		}
		return 0;
	}
	if (!(S_ISREG(source_stat.st_mode) || S_ISDIR(source_stat.st_mode))) {
		printf("Warning: currently copying link or device is not supported, " 
				"'%s' is omitted\n", source);
	}
	return 0;
}

void usage(char **argv) {
	printf("Usage: %s [-f] [-r] [-t thread_num] SOURCE DEST\n", argv[0]);
}

int main(int argc, char **argv) {
	int c;
	int copy_flags = 0;
	int thread_num = 1;
	int ret_val = 0;
	while ((c = getopt(argc, argv, "frt:")) != -1) {
		switch (c) {
			case 'f':
				copy_flags |= FORCE_COPY_FLAG;
				break;
			case 'r':
				copy_flags |= RECURSIVE_COPY_FLAG;
				break;
			case 't':
				thread_num = atoi(optarg);
				break;
		}
	}
	if ((argc - optind) < 2) {
		usage(argv);
		return -1;
	}
	argv += optind;
	argc -= optind;
	const char *last = argv[argc - 1];
	const char *dest;
	char *source;
	struct stat last_stat;
	struct workqueue wq;
	wq.head = (struct item_node*)malloc(sizeof(struct item_node));
	wq.head->cur_chunk = NULL;
	wq.head->next = NULL;
	wq.tail = wq.head;
	pthread_spin_init(&wq.s_lock, PTHREAD_PROCESS_PRIVATE);	
	if (argc == 2) {
		source = *argv;
		if (lstat(last, &last_stat) < 0) {
			if (errno != ENOENT) {
				printf("cant't stat '%s', something unexpected", dest);
				return -1;
			}

		}
		if (S_ISDIR(last_stat.st_mode)) {
			dest = concat_path_file(last, get_last_component(*argv));
		} else {
			dest = last;
		}
		ret_val = generate_tasks(source, dest, copy_flags, &wq);
		if (dest != last) {
			free((void*)dest);
		}
		if (ret_val < 0) { 
			free(wq.head);
			return -1;
		}
	} else {
		while (1) {
			source = *argv;
			dest = concat_path_file(last, get_last_component(source));
			ret_val = generate_tasks(source, dest, copy_flags, &wq);
			if (ret_val < 0) {
				free((void*)dest);
				free(wq.head);
				return -1;
			}
			argv++;
			if(*argv == last) {
				free((void*)dest);
				break;
			}
			free((void*)dest);
		}
	}
	pthread_t pthread_ids[MAX_THREADS];
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	int i = 0, j = 0, pr;
	for (i = 0; i < thread_num; ++i) {
		pr = pthread_create(&pthread_ids[i], &attr, copy_task, (void*)(&wq));
		if (pr) {
			printf("Error; return code from pthread_create: %d\n", pr);
			return -1;
		}
	}
	for (j = 0; j < thread_num; ++j) {
		int exret;
		int rc = pthread_join(pthread_ids[j], (void*)&exret);
		if (rc) {
			printf("Error; pthread_join return error code: %d\n", rc);
			return -1;
		}
		if(exret < 0) {
			free(wq.head);
			return -1;
		}
	}
	pthread_attr_destroy(&attr);
	pthread_spin_destroy(&wq.s_lock);
	free(wq.head);
	if (ret_val < 0) {
		printf("copy failed for unexpected error!\n");
		return -1;
	}
	return 0;
}
