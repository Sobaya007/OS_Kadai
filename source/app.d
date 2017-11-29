import dfuse.fuse, c.fuse.common;

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
        } else if (file !is null) {
            s.st_mode = S_IFREG | octal!777;
            s.st_size = file.size;
            s.st_nlink = file.nlink;
            s.st_mtime = Clock.currTime.toUnixTime;
            return;
        }
        throw new FuseException(errno.ENOENT);
    }

    override string[] readdir(const(char)[] path)
    {
        auto file = fs.getFile(path.to!string);
        if (auto dir = cast(Directory)file) {
            auto names = dir.iterator.map!(entry => entry.name).array;
            return names;
        }

        throw new FuseException(errno.ENOENT);
    }

    override ulong read(const(char)[] path, ubyte[] buf, ulong offset)
    {
        auto file = fs.getFile(path.to!string);
        auto size = file.size - offset;
        buf[0..size] = file.getContent[offset..offset+size];
        return size;
    }

    override int write(const(char)[] path, in ubyte[] data, ulong offset)
    {
        auto file = fs.getFile(path.to!string);
        file.size = cast(ushort)(offset + data.length);
        return 0;
        //return file.writeContent(data, offset);
    }

    override bool access(const(char)[] path, int mode)
    {
        return true;
    }

    override void truncate(const(char)[] path, ulong length)
    {
        auto file = fs.getFile(path.to!string);
        if (file is null) throw new FuseException(errno.ENOENT);
        file.size = cast(uint)length;
    }
}

enum BLOCK_SIZE = 512;
enum NDIRECT = 12;
enum DIRSIZ = 14;

class File {

    private FileSystem fs;
    short major;
    short minor;
    short nlink;
    uint size;
    uint[NDIRECT+1] addrs;

    this(FileSystem fs, dinode* inode) {
        this.fs = fs;
        this.nlink = inode.nlink;
        this.size = inode.size;
        this.addrs = inode.addrs;
    }

    ubyte[] getContent() {
        ubyte[] content;
        auto nblocks = (size-1) / BLOCK_SIZE + 1;
        if (nblocks <= NDIRECT) {
            foreach (j; 0..nblocks) {
                content ~= fs.getBlock(addrs[j]);
            }
        } else {
            foreach (j; 0..NDIRECT) {
                content ~= fs.getBlock(addrs[j]);
            }
            auto indirectBlock = cast(uint*)fs.getBlock(addrs[NDIRECT]);
            foreach (j; 0..nblocks - NDIRECT) {
                content ~= fs.getBlock(indirectBlock[j]);
            }
        }
        return content;
    }
}

alias DirEntry = Tuple!(string, "name", ushort, "inum");

class Directory : File {

    this(FileSystem fs, dinode *inode) {
        super(fs, inode);
    }

     auto iterator() {
         auto entries = cast(dirent*)getContent;
         auto entryNum = size / dirent.sizeof;
         class Iterator {

             bool empty() {
                 return entryNum == 0 || entries[0].name[0] == 0;
             }

             DirEntry front() {
                 auto entry = entries[0];
                 auto name = entry.name[].filter!(c => c != '\0').to!string;
                 return DirEntry(name, entry.inum);
             }

             void popFront() {
                 entries++;
                 entryNum--;
             }
         }
         return new Iterator;
     }
}

struct superblock {
    uint size;
    uint nblocks;
    uint ninodes;
    uint nlogs;
    uint logstart;
    uint inodestart;
    uint bitmapstart;
}

struct dinode {
    short type;
    short major;
    short minor;
    short nlink;
    uint size;
    uint[NDIRECT+1] addrs;
}

struct dirent {
    ushort inum;
    char[DIRSIZ] name;
}

class FileSystem {

    superblock *sblock;
    ubyte[] buf;

    this(string imagepath) {
        this.buf = cast(ubyte[])read("fs.img");
        auto cur = buf;

        alias consume = n => cur = cur[BLOCK_SIZE * n..$];

        // first block
        assert(cur[0..BLOCK_SIZE].all!(b => b == 0));
        consume(1);

        // super block
        this.sblock = cast(superblock*)&cur[0];
        consume(1);

        // log block
        assert(cur == buf[BLOCK_SIZE * sblock.logstart..$]);
        consume(sblock.nlogs);

        // inode block
        enum IPB = BLOCK_SIZE / dinode.sizeof;
        auto ninodeblocks = sblock.ninodes / IPB + 1;
        consume(ninodeblocks);

        // bitmap block
        assert(cur == buf[BLOCK_SIZE * sblock.bitmapstart..$]);
        auto bitmaps = cast(bool*)&cur[0];
        auto nbitmapblocks = sblock.size / (BLOCK_SIZE * IPB) + 1;
        consume(nbitmapblocks);

        // data block
        assert(sblock.size - 2 - ninodeblocks - nbitmapblocks - sblock.nlogs == sblock.nblocks);
        auto data = &cur[0];
    }

    uint inodeNum() {
        return sblock.ninodes;
    }

    uint blockNum() {
        return sblock.nblocks;
    }

    ubyte[] getBlock(uint n) in {
        assert (n <= blockNum);
    } body {
        return buf[n*BLOCK_SIZE .. (n+1)*BLOCK_SIZE];
    }

    ubyte[] getBlocks(uint offset) in {
        assert(offset <= blockNum);
    } body {
        return buf[offset*BLOCK_SIZE..$];
    }

    dinode* getInode(uint inum) in {
        assert(inum < inodeNum);
    } body {
        return cast(dinode*)getBlocks(sblock.inodestart).ptr + inum;
    }

    File getFile(string path) in {
        assert(path[0] == '/');
    } body {
        auto rootInode = getInode(1);
        File cur = new Directory(this, rootInode);
        assert(path.split("/").length < 5);
        foreach (p; path.split("/")) {
            if (p.empty) continue;
            auto dir = cast(Directory)cur;
            if (dir is null) return null;
            auto findResult = dir.iterator.find!(entry => entry.name == p);
            if (findResult.empty) return null;
            auto inode = getInode(findResult.front.inum);
            if (inode.type == 1) {
                cur = new Directory(this, inode);
            } else if (inode.type == 2) {
                cur = new File(this, inode);
            } else {
                assert(false);
            }
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
