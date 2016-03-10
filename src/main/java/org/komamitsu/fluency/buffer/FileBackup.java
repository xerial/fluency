package org.komamitsu.fluency.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class FileBackup
{
    private static final Logger LOG = LoggerFactory.getLogger(FileBackup.class);
    private final File backupDir;
    private final Buffer buffer;
    private final FileFilter fileFilter;

    public static class SavedBuffer
        implements Closeable
    {
        private final File savedFile;
        private FileChannel channel;

        public SavedBuffer(File savedFile)
        {
            this.savedFile = savedFile;
        }

        public File getSavedFile()
        {
            return savedFile;
        }

        public ByteBuffer open()
                throws IOException
        {
            channel = new RandomAccessFile(savedFile, "r").getChannel();
            return channel.map(FileChannel.MapMode.PRIVATE, 0, savedFile.length());
        }

        public void success()
        {
            try {
                close();
            }
            catch (IOException e) {
                LOG.warn("Failed to close file=" + savedFile, e);
            }
            finally {
                if (!savedFile.delete()) {
                    LOG.warn("Failed to delete file=" + savedFile);
                }
            }
        }

        @Override
        public void close()
                throws IOException
        {
            if (channel != null) {
                channel.close();
                channel = null;
            }
        }
    }

    public FileBackup(File backupDir, Buffer buffer)
    {
        this.backupDir = backupDir;
        this.buffer = buffer;
        final Pattern pattern = Pattern.compile(buffer.bufferType() + "_\\d+\\.buf");
        this.fileFilter = new FileFilter() {
            @Override
            public boolean accept(File pathname)
            {
                return pattern.matcher(pathname.getName()).find();
            }
        };
    }

    public List<SavedBuffer> getSavedFiles()
    {
        File[] files = backupDir.listFiles(fileFilter);
        ArrayList<SavedBuffer> savedBuffers = new ArrayList<SavedBuffer>(files.length);
        for (File f : files) {
            savedBuffers.add(new SavedBuffer(f));
        }
        // TODO: Make sure the user calls close() or success() for each SavedBuffer...
        return savedBuffers;
    }
}
