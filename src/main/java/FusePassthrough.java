import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import jnr.ffi.types.mode_t;
import jnr.ffi.types.off_t;
import jnr.ffi.types.size_t;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Timespec;
import java.io.File;
import java.nio.file.Paths;
import java.util.logging.Logger;

public class FusePassthrough extends FuseStubFS {

    Logger logger;
    POSIX posix;
    final String mount_path = "/home/cody/Desktop/mntp";


    public static void main(String[] args) {
        FusePassthrough fusePT = new FusePassthrough();
        try {
            fusePT.mount(Paths.get(fusePT.mount_path), true, true);
        } finally {
            fusePT.umount();
        }
    }

    public FusePassthrough() {
        posix = POSIXFactory.getPOSIX();
        logger = Logger.getLogger(FusePassthrough.class.getName());
    }

    @Override
    public int getattr(String path, FileStat stat) {
        System.out.printf("Called getattr, with path %s\n", path);
        jnr.posix.FileStat jnrStat = posix.allocateStat();
        int result = posix.lstat(path, jnrStat);
        translateFileStat(jnrStat, stat);
        if (result < 0) {
            result = -posix.errno();
        }
        return result;
    }

    @Override
    public int mkdir(String path, @mode_t long mode) {
        System.out.printf("Called mkdir with path %s\n", path);
        int result = posix.mkdir(path, (int)mode);
        if (result < 0) {
            result = -posix.errno();
        }
        return result;
    }

    @Override
    public int rmdir(String path) {
        System.out.printf("Called rmdir wih path %s\n", path);
        int result = posix.rmdir(path);
        if (result < 0) {
            result = -posix.errno();
        }
        return result;
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filter, @off_t long offset, FuseFileInfo fi) {
        System.out.printf("Called readdir with path %s\n", path);
        if (!new File(path).exists()) {
            return -ErrorCodes.ENOENT();
        }
        File[] files = new File(path).listFiles();
        for (File file: files) {
            filter.apply(buf, file.getName(), null, 0);
        }
        return 0;
    }

    @Override
    public int create(String path, @mode_t long mode, FuseFileInfo fi) {
        System.out.printf("Called create with path %s\n", path);
        if (new File(path).exists()) {
            return -ErrorCodes.EEXIST();
        }
        //This statement is apparently equivalent to create() but may use fi.flags instead if issues arise
        int result = posix.open(path,OpenFlags.O_CREAT.intValue() | OpenFlags.O_WRONLY.intValue() | OpenFlags.O_TRUNC.intValue(), (int)mode);
        if (result < 0) {
            result = -posix.errno();
        }
        return 0;
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        System.out.printf("Called open with path %s\n", path);
        int result = posix.open(path, fi.flags.intValue(), OpenFlags.O_RDONLY.intValue());
        if (result < 0) {
            return -posix.errno();
        }
        posix.close(result);
        return 0;
    }

    @Override
    public int chmod(String path, long mode) {
        System.out.printf("Called chmod with path %s\n", path);
        int result = posix.chmod(path, (int)mode);
        if (result < 0) {
            return -posix.errno();
        }
        return result;
    }

    @Override
    public int chown(String path, long uid, long gid) {
        System.out.printf("Called chown with path %s\n", path);
        int result = posix.chown(path, (int)uid, (int)gid);
        if (result < 0) {
            return -posix.errno();
        }
        return result;
    }

    @Override
    public int truncate(String path, long size) {
        System.out.printf("Called truncate with path %s\n", path);
        int result = posix.truncate(path, size);
        if (result < 0) {
            return -posix.errno();
        }
        return result;
    }

    @Override
    public int unlink(String path) {
        System.out.printf("Called unlink with path %s\n", path);
        int result = posix.unlink(path);
        if (result < 0) {
            return -posix.errno();
        }
        return result;
    }

    @Override
    public int rename(String oldpath, String newpath) {
        System.out.printf("Called rename with oldpath: %s  newpath: %s\n", oldpath, newpath);
        int result = posix.rename(oldpath, newpath);
        if (result < 0) {
            return -posix.errno();
        }
        return result;
    }

    @Override
    public int utimens(String path, Timespec[] timespec) {
        System.out.printf("Called utimens with path %s\n", path);
        long[] atimeval = {timespec[0].tv_sec.get(), timespec[0].tv_nsec.longValue() / 1000};
        long[] mtimeval = {timespec[1].tv_sec.get(), timespec[1].tv_nsec.longValue() / 1000};
        int result = posix.utimes(path, atimeval, mtimeval);
        if (result < 0) {
            return -posix.errno();
        }
        return result;
    }

    @Override
    public int read(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        System.out.printf("Called read with path %s\n", path);
        int fd = posix.open(path, fi.flags.intValue(), OpenFlags.O_RDONLY.intValue());
        if (fd < 0) {
            return -posix.errno();
        }
        byte[] buffer = new byte[(int) size];
        int result = (int) posix.pread(fd, buffer, size, offset);
        buf.put(0, buffer, 0, (int)size);
        if (result < 0) {
            result = -posix.errno();
        }
        posix.close(fd);
        return result;
    }

    @Override
    public int write(String path, Pointer buf, @size_t long size, @off_t long offset, FuseFileInfo fi) {
        System.out.printf("Called write with path %s\n", path);
        int fd = posix.open(path, fi.flags.intValue(), OpenFlags.O_RDWR.intValue());
        if (fd < 0) {
            return -posix.errno();
        }
        byte[] buffer = new byte[(int)size];
        buf.get(0, buffer, 0, (int)size);
        int result = (int)posix.pwrite(fd, buffer, size, offset);
        if (result < 0) {
            result = -posix.errno();
        }
        posix.close(fd);
        return result;
    }

    //There's probably a better way to do this.
    public void translateFileStat(jnr.posix.FileStat jnrStat, FileStat fuseStat) {
        fuseStat.st_atim.tv_sec.set(jnrStat.atime());
        fuseStat.st_blksize.set(jnrStat.blockSize());
        fuseStat.st_blocks.set(jnrStat.blocks());
        fuseStat.st_ctim.tv_sec.set(jnrStat.ctime());
        fuseStat.st_dev.set(jnrStat.dev());
        fuseStat.st_gid.set(jnrStat.gid());
        fuseStat.st_ino.set(jnrStat.ino());
        fuseStat.st_mode.set(jnrStat.mode());
        fuseStat.st_mtim.tv_sec.set(jnrStat.mtime());
        fuseStat.st_nlink.set(jnrStat.nlink());
        fuseStat.st_rdev.set(jnrStat.rdev());
        fuseStat.st_size.set(jnrStat.st_size());
        fuseStat.st_uid.set(jnrStat.uid());
    }
}
