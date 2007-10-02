#include <device.h>
#include <amanda.h>
#include <timestamp.h>

int blocksize;
unsigned int seed = 0;

static char * make_rand_buf(int size) {
    char * rval;
    unsigned int i;

    rval = malloc(size);
    i = size;
    while (i > sizeof(int)) {
        int rand;
        rand = rand_r(&seed);
        memcpy(rval + size - i, &rand, sizeof(int));
        i -= sizeof(int);
    }
    
    if (size > 0) {
        int rand;
        rand = rand_r(&seed);
        memcpy(rval + size - i, &rand, i);
    }

    return rval;
}

static gboolean write_whole_file(Device * device) {
    dumpfile_t dumpfile;
    char * tmp;
    int i;

    fh_init(&dumpfile);
    dumpfile.type = F_DUMPFILE;
    tmp = get_timestamp_from_time(time(NULL));
    strcpy(dumpfile.datestamp, tmp);
    amfree(tmp);
    strcpy(dumpfile.name, "localhost");
    tmp = g_get_current_dir();
    strcpy(dumpfile.disk, tmp);
    amfree(tmp);
    strcpy(dumpfile.program, "TESTER");
    strcpy(dumpfile.recover_cmd, "recover_cmd");

    blocksize = device_write_max_size(device);
    
    g_return_val_if_fail(device_start_file(device, &dumpfile), FALSE);
    
    for (i = 0; i < 1000; i ++) {
        int size;
        char * buf;
        if (i == 999)
            size = blocksize / 2;
        else
            size = blocksize;
        buf = make_rand_buf(size);
        g_return_val_if_fail(device_write_block(device, size, buf, i == 999),
                             FALSE);
        amfree(buf);
    }
    
    g_return_val_if_fail(device->in_file == FALSE, FALSE);

    return TRUE;
}

static gboolean read_whole_file(Device * device, int fileno) {
    int size = 0;
    dumpfile_t * file = device_seek_file(device, fileno + 1);
    int i;
    char *buf;

    if (file == NULL)
        g_assert_not_reached();
    else
        amfree(file);

    g_return_val_if_fail(device_seek_block(device, 0), FALSE);
    
    g_return_val_if_fail(0 == device_read_block(device, NULL, &size),
                         FALSE);
    g_assert(size >= blocksize);
        
    for (i = 0; i < 1000; i ++) {
        int size, size2;
        char buf2[blocksize];
        size2 = blocksize;
        if (i == 999)
            size = blocksize/2;
        else
            size = blocksize;
        buf = make_rand_buf(size);
        
        g_return_val_if_fail(device_read_block(device, buf2, &size2),
                             FALSE);
        g_assert(size2 == size || size2 == blocksize);
        g_assert(memcmp(buf, buf2, size) == 0);
        amfree(buf);
    }
    
    size = blocksize;
    buf = malloc(blocksize);
    g_assert(-1 == device_read_block(device, &buf, &size));
    g_return_val_if_fail(device->is_eof, FALSE);
    free(buf);

    return TRUE;
}

static MediaAccessMode get_medium_type(Device * device) {
    GValue value;
    MediaAccessMode rval;
    
    bzero(&value, sizeof(value));

    g_return_val_if_fail(device_property_get(device, PROPERTY_MEDIUM_TYPE,
                                             &value), 0);

    rval = g_value_get_enum(&value);
    g_value_unset(&value);
    return rval;
}

int main(int argc, char ** argv) {
    Device * device;
    int h;
    MediaAccessMode medium_type;
    
    g_return_val_if_fail(argc == 2, 1);

    device_api_init();

    device = device_open(argv[1]);
    g_return_val_if_fail(device != NULL, 2);

    medium_type = get_medium_type(device);

    if (device->volume_label) {
        printf("Last header: %s %s\n", device->volume_label,
               device->volume_time);
    }

    if (medium_type != MEDIA_ACCESS_MODE_READ_ONLY) {
        g_return_val_if_fail(device_start(device, ACCESS_WRITE, 
                                          "foo", NULL),
                             2);
        
        for (h = 0; h < 10; h ++) {
            gboolean appendable;
            GValue value;
            g_return_val_if_fail(write_whole_file(device), 3);
            
            bzero(&value, sizeof(value));
            g_return_val_if_fail(device_property_get(device,
                                                     PROPERTY_APPENDABLE,
                                                     &value), 4);
            appendable = g_value_get_boolean(&value);
            g_value_unset(&value);
            
            if (appendable && h == 5) {
                g_object_unref(device);
                
                device = device_open(argv[1]);
                g_return_val_if_fail(device != NULL, 6);
                
                g_return_val_if_fail(device_start(device, ACCESS_APPEND, 
                                                  "foo", NULL),
                                     2);
            }
        }
        
        g_object_unref(device);    
        
        device = device_open(argv[1]);
        g_return_val_if_fail(device != NULL, 6);
    }

    /* Fixme: check for readable access mode. */
    if (medium_type != MEDIA_ACCESS_MODE_WRITE_ONLY) {
        g_return_val_if_fail(device->volume_label, 7);
        printf("This header: %s %s\n", device->volume_label,
               device->volume_time);    
        
        g_return_val_if_fail(device_start(device, ACCESS_READ, 
                                          "foo", NULL),
                             2);
        seed = 0;
        for (h = 0; h < 10; h ++) {
            g_return_val_if_fail(read_whole_file(device, h), 8);
        }
    }

    g_object_unref(device);    

    return 0;
}
