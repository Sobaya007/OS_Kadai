import dfuse.fuse;

import std.algorithm, std.conv, std.stdio, std.file, std.string, std.range, std.array, std.typecons, std.datetime;

class MyFS : Operations
{
    private FileSystem fs;

    this(FileSystem fs) {
        this.fs = fs;
    }

    override void getattr(const(char)[] path, ref stat_t s)
    {
        auto file = fs.getFile(path.to!string);

        if (auto dir = cast(Directory)file) {
            s.st_mode = S_IFDIR | octal!755;
            s.st_size = dir.size;
            s.st_nlink = dir.nlink;
            s.st_mtime = Clock.currTime.toUnixTime;
            return;
        } else if (auto f = cast(FileContent)file) {
            s.st_mode = S_IFREG | octal!777;
            s.st_size = f.size;
            s.st_nlink = f.nlink;
            s.st_mtime = Clock.currTime.toUnixTime;
            return;
        }

        throw new FuseException(errno.ENOENT);
    }

    override string[] readdir(const(char)[] path)
    {
        auto file = fs.getFile(path.to!string);
        if (auto dir = cast(Directory)file) {
            auto names = dir.entries.map!(entry => entry.name).array;
            return names;
        }

        throw new FuseException(errno.ENOENT);
    }
}

enum NDIRECT = 12;
enum DIRSIZ = 14;

interface File {
}

class FileContent : File {
    short major;
    short minor;
    short nlink;
    uint size;
    ubyte[] content;
}

alias DirEntry = Tuple!(string, "name", ushort, "inum");

class Directory : File {
     DirEntry[] entries;
     short nlink;
     uint size;
}

class FileSystem {

    enum BLOCK_SIZE = 512;

    private File[] files;
    private Directory root;

    this(string imagepath) {
        auto buf = cast(ubyte[])read("fs.img");
        auto cur = buf;

        alias consume = n => cur = cur[BLOCK_SIZE * n..$];

        // first block
        assert(cur[0..BLOCK_SIZE].all!(b => b == 0));
        consume(1);

        // super block
        struct SuperBlock {
            uint size;
            uint nblocks;
            uint ninodes;
            uint nlogs;
            uint logstart;
            uint inodestart;
            uint bitmapstart;
        }
        auto superBlock = cast(SuperBlock*)&cur[0];
        consume(1);

        // log block
        assert(cur == buf[BLOCK_SIZE * superBlock.logstart..$]);
        consume(superBlock.nlogs);

        // inode block
        struct dinode {
            short type;
            short major;
            short minor;
            short nlink;
            uint size;
            uint[NDIRECT+1] addrs;
        }
        enum IPB = BLOCK_SIZE / dinode.sizeof;
        auto inodes = cast(dinode*)&cur[0];
        auto ninodeblocks = superBlock.ninodes / IPB + 1;
        consume(ninodeblocks);

        // bitmap block
        assert(cur == buf[BLOCK_SIZE * superBlock.bitmapstart..$]);
        auto bitmaps = cast(bool*)&cur[0];
        auto nbitmapblocks = superBlock.size / (BLOCK_SIZE * IPB) + 1;
        consume(nbitmapblocks);

        // data block
        assert(superBlock.size - 2 - ninodeblocks - nbitmapblocks - superBlock.nlogs == superBlock.nblocks);
        auto data = &cur[0];

        // construct file info
        this.files.length = superBlock.ninodes;
        foreach (i; 0..superBlock.ninodes) {
            auto inode = inodes[i];

            if (inode.type == 0) continue;

            auto nblocks = (inode.size-1) / BLOCK_SIZE + 1;
            assert(nblocks <= NDIRECT + BLOCK_SIZE / uint.sizeof );


            ubyte[] content;
            if (nblocks <= NDIRECT) {
                foreach (j; 0..nblocks) {
                    auto block = inode.addrs[j];
                    content ~= buf[block * BLOCK_SIZE..(block+1) * BLOCK_SIZE];
                }
            } else {
                foreach (j; 0..NDIRECT) {
                    auto block = inode.addrs[j];
                    content ~= buf[block * BLOCK_SIZE..(block+1) * BLOCK_SIZE];
                }
                auto indirectBlock = cast(uint*)&buf[inode.addrs[NDIRECT] * BLOCK_SIZE];
                foreach (j; 0..nblocks - NDIRECT) {
                    auto block = indirectBlock[j];
                    content ~= buf[block * BLOCK_SIZE..(block+1) * BLOCK_SIZE];
                }
            }
            if (inode.type == 1) {
                struct dirent {
                    ushort inum;
                    char[DIRSIZ] name;
                }
                auto dirs = cast(dirent*)content;
                auto dirnum = inode.size / dirent.sizeof;
                auto dir = new Directory;
                foreach (j; 0..dirnum) {
                    auto entry = dirs[j];
                    if (entry.inum == 0) continue;
                    auto name = entry.name[].filter!(c => c != '\0').to!string;
                    dir.entries ~= DirEntry(name, entry.inum);
                }
                dir.entries = dir.entries.sort!((a,b) => a.name < b.name).array;
                dir.size = cast(uint)(dir.entries.length * dirent.sizeof);
                dir.nlink = cast(ushort)(dir.entries.length - 2 + 1);
                this.files[i] = dir;
            } else if (inode.type == 2) {
                auto file = new FileContent;
                file.major = inode.major;
                file.minor = inode.minor;
                file.nlink = inode.nlink;
                file.size = inode.size;
                file.content = content;
                this.files[i] = file;
            } else {
                assert(false);
            }
        }

        auto findResult = files.find!(file => cast(Directory)file);
        assert(!findResult.empty);
        this.root = cast(Directory)findResult[0];
        assert(this.root !is null);
        while (true) {
            auto findResult2 = this.root.entries.find!(entry => entry.name == "..");
            assert(!findResult2.empty);
            auto parent = cast(Directory)files[findResult2[0].inum];
            assert(parent !is null);
            if (this.root is parent) break;
            this.root = parent;
        }
    }

    Directory getRoot() {
        return this.root;
    }

    File[] getChildren(Directory dir) {
        return dir.entries.map!(entry => files[entry.inum]).array;
    }

    File getFile(string path) in {
        assert(path[0] == '/');
    } body {
        File cur = this.root;
        assert(path.split("/").length < 5);
        foreach (p; path.split("/")) {
            if (p.empty) continue;
            auto dir = cast(Directory)cur;
            if (dir is null) return null;
            auto findResult = dir.entries.find!(entry => entry.name == p);
            if (findResult.empty) return null;
            cur = this.files[findResult[0].inum];
        }
        return cur;
    }
}

int main(string[] args)
{
    if (args.length != 2)
    {
        stderr.writeln("simplefs <MOUNTPOINT>");
        return -1;
    }

    stdout.writeln("mounting simplefs");

    auto fs = new Fuse("SimpleFS", true, false);
    fs.mount(new MyFS(new FileSystem("fs.img")), args[1], []);

    return 0;
}
