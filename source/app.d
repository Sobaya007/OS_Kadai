import dfuse.fuse, c.fuse.common;

import std.algorithm, std.conv, std.stdio, std.file, std.string, std.range, std.array, std.typecons, std.datetime, std.mmfile, std.bitmanip;

class MyFS : Operations
{
    private FileSystem fs;
    private long startTime;

    this(FileSystem fs) {
        this.fs = fs;
        this.startTime = Clock.currTime.toUnixTime;
    }

    override void getattr(const(char)[] path, ref stat_t s)
    {
        auto file = fs.getFile(path.to!string);

        if (auto dir = cast(Directory)file) {
            s.st_mode = S_IFDIR | octal!755;
            s.st_size = dir.inode.size;
            s.st_nlink = dir.inode.nlink;
            s.st_mtime = this.startTime;
            return;
        } else if (file !is null) {
            s.st_mode = S_IFREG | octal!777;
            s.st_size = file.inode.size;
            s.st_nlink = file.inode.nlink;
            s.st_mtime = this.startTime;
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
        auto size = file.inode.size - offset;
        buf[0..size] = file.getContent[offset..offset+size];
        return size;
    }

    override int write(const(char)[] path, in ubyte[] data, ulong offset)
    {
        auto file = fs.getFile(path.to!string);
        file.inode.size = cast(ushort)(offset + data.length);
        return file.writeContent(data, offset);
    }

    override void truncate(const(char)[] path, ulong length)
    {
        auto file = fs.getFile(path.to!string);
        if (file is null) throw new FuseException(errno.ENOENT);
        file.resize(length);
    }
}

enum BLOCK_SIZE = 512;
enum NDIRECT = 12;
enum DIRSIZ = 14;

class File {

    private FileSystem fs;
    dinode *inode;

    this(FileSystem fs, dinode* inode) {
        this.fs = fs;
        this.inode = inode;
    }

    ubyte[] getContent() {
        ubyte[] content;
        auto nblocks = (inode.size-1) / BLOCK_SIZE + 1;
        foreach (i; 0..nblocks) {
            content ~= fs.getBlock(address(i));
        }
        return content;
    }

    private ref uint address(ulong index) {
        if (index < NDIRECT) return inode.addrs[index];
        auto indirectBlock = cast(uint*)fs.getBlock(inode.addrs[NDIRECT]);
        return indirectBlock[index - NDIRECT];
    }

    int writeContent(in ubyte[] data, ulong offset) {
        resize(offset + data.length);
        auto dataOffset = 0;
        auto index = cast(uint)offset / BLOCK_SIZE;
        offset -= index * BLOCK_SIZE;
        while (dataOffset < data.length) {
            auto block = fs.getBlock(address(index));
            auto writeSize = min(BLOCK_SIZE-offset, data.length - dataOffset);
            block[offset..offset+writeSize] = data[dataOffset..dataOffset+writeSize];

            index++;
            offset = 0;
            dataOffset += writeSize;
        }
        return dataOffset;
    }

    void resize(ulong length) {
        inode.size = cast(uint)length;
        auto blockNum = length / BLOCK_SIZE + 1;


        // manage indirect block
        if (blockNum > NDIRECT && inode.addrs[NDIRECT] == 0) {
            // allocate
            auto indirectBlockIndex = fs.getEmptyDataBlockIndex();
            inode.addrs[NDIRECT] = indirectBlockIndex;
            fs.setBitmap(indirectBlockIndex, true);
        }
        if (blockNum <= NDIRECT && inode.addrs[NDIRECT] != 0) {
            // release
            //fs.setBitmap(inode.addrs[NDIRECT], false);
            //inode.addrs[NDIRECT] = 0;
        }

        // allocate
        foreach (i; 0..blockNum) {
            if (address(i) != 0) continue;
            auto newBlockIndex = fs.getEmptyDataBlockIndex();
            address(i) = newBlockIndex;
            fs.setBitmap(newBlockIndex, true);
        }

        // release
        foreach (i; blockNum..NDIRECT+BLOCK_SIZE / uint.sizeof) {
            if (address(i) == 0) break;
            //fs.setBitmap(address(i), false);
            //address(i) = 0;
        }
    }
}

alias DirEntry = Tuple!(string, "name", ushort, "inum");

class Directory : File {

    this(FileSystem fs, dinode *inode) {
        super(fs, inode);
    }

     auto iterator() {
         auto entries = cast(dirent*)getContent;
         auto entryNum = inode.size / dirent.sizeof;
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

class Bitmap {
    private ubyte[] bitmaps;
    private size_t begin, end;

    this(ubyte[] bitmaps) {
        this.bitmaps = bitmaps;
        this.begin = 0;
        this.end = bitmaps.length * 8;
    }

    bool opIndex(size_t idx) in {
        assert(begin + idx < end);
    } body {
        idx += begin;
        return cast(bool)(bitmaps[idx / 8] & (1 << (7 - idx % 8)));
    }

    void opIndexAssign(bool b, size_t idx) in {
        assert(begin + idx < end);
    } body {
        idx += begin;
        if (b) {
            bitmaps[idx / 8] |= (1 << (7 - idx % 8));
        } else {
            bitmaps[idx / 8] &= ~(1 << (7 - idx % 8));
        }
    }

    size_t length() {
        return end - begin;
    }

    auto iterator() {
        auto pos = 0;
        class Iterator {
            bool empty() {
                return pos >= length;
            }

            bool front() {
                return opIndex(pos);
            }

            void popFront() {
                pos++;
            }
        }
        return new Iterator;
    }

    Bitmap opSlice(size_t begin, size_t end) in {
    } body {
        auto res = new Bitmap(bitmaps);
        res.begin = this.begin + begin;
        res.end = this.begin + end;
        return res;
    }
}

class FileSystem {

    superblock *sblock;
    ubyte[] buf;
    MmFile file;

    this(string imagepath) {
        this.file = new MmFile("fs.img", MmFile.Mode.readWrite, 0, null);
        this.buf = cast(ubyte[])file[];

        // super block
        this.sblock = cast(superblock*)buf[BLOCK_SIZE..$].ptr;
    }

    uint inodeNum() {
        return sblock.ninodes;
    }

    uint blockNum() {
        return sblock.nblocks;
    }

    ubyte[] getBlock(uint n) {
        return getBlocks(n)[0..BLOCK_SIZE];
    }

    ubyte[] getBlocks(uint offset) in {
        assert(0 < offset);
        assert(offset < blockNum);
    } body {
        return buf[offset*BLOCK_SIZE..$];
    }

    dinode* getInode(uint inum) in {
        assert(inum < inodeNum);
    } body {
        return cast(dinode*)getBlocks(sblock.inodestart).ptr + inum;
    }

    uint getEmtpyInodeBlockIndex() {
        return getEmptyBlockIndex(sblock.inodestart, sblock.bitmapstart);
    }

    uint getEmptyDataBlockIndex() {
        auto nbitmapblocks = sblock.size / (BLOCK_SIZE * ubyte.sizeof) + 1;
        auto datastart = sblock.bitmapstart + nbitmapblocks;
        return getEmptyBlockIndex(datastart, sblock.size);
    }

    File getFile(string path) in {
        assert(path[0] == '/');
    } body {

        auto rootInode = getInode(1);
        File cur = new Directory(this, rootInode);

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

    void setBitmap(ulong idx, bool b) {
        auto nbitmapblocks = sblock.size / (BLOCK_SIZE * 8) + 1;
        auto bitmap = new Bitmap(getBlocks(sblock.bitmapstart)[0..nbitmapblocks * BLOCK_SIZE]);
        bitmap[idx] = b;
    }

    private uint getEmptyBlockIndex(ulong begin, ulong end) {
        auto nbitmapblocks = sblock.size / (BLOCK_SIZE * 8) + 1;
        auto bitmap = new Bitmap(getBlocks(sblock.bitmapstart)[0..nbitmapblocks * BLOCK_SIZE]);
        bitmap = bitmap[begin..end];
        auto pos = bitmap.iterator.countUntil(false);
        assert (pos >= 0);
        return cast(uint)(begin + pos);
    }

}

int main(string[] args)
{
    if (args.length != 3)
    {
        stderr.writeln(args[0], " <MOUNTPOINT> <FILESYSTEM_IMAGE>");
        return -1;
    }

    stdout.writeln("mounting ", args[0]);

    auto fs = new Fuse(args[0], true, false);
    fs.mount(new MyFS(new FileSystem(args[2])), args[1], []);

    return 0;
}
