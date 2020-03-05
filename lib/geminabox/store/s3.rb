module Geminabox
  module Store
    class S3
      attr_reader :logger

      GZIPPED_FILES = %w[
        specs.4.8.gz
        latest_specs.4.8.gz
        prerelease_specs.4.8.gz
      ]
      TEXT_FILES = %w[
        yaml
        Marshal.4.8
        specs.4.8
        latest_specs.4.8
        prerelease_specs.4.8
      ]

      def initialize(params={})
        @bucket       = params[:bucket]
        @file_store   = params[:file_store] || Geminabox::GemStore
        @lock_manager = params[:lock_manager]
        @logger       = params[:logger] || Logger.new(STDOUT)

        @local_metadata_lock = Mutex.new
        @local_and_remote_metadata_lock = Mutex.new
        # Check if local metadata needs an update from S3 on start.
        update_local_metadata
      end

      def create(gem, overwrite = false)
        @local_and_remote_metadata_lock.synchronize do
          @lock_manager.lock "geminabox-upload" do
            update_local_metadata

            @file_store.create gem, overwrite

            object_name = gem_object_name("/gems/" + gem.name)

            logger.info "Gem: local -> S3 #{object_name}"

            @bucket
              .object(object_name)
              .write gem.gem_data
            update_metadata
          end
        end
      end

      def access_metadata
        logger.info "access_metadata hook"
        update_local_metadata
        logger.info "access_metadata hook completed"
      end

      # Get the last file from TEXT_FILES (last upload that takes place),
      # and compare local vs remote timestamps.
      # true if remote is newer than local.
      def check_if_remote_metadata_changed
        object_to_check = TEXT_FILES.last
        remote_last_modified = nil
        begin
          remote_last_modified = @bucket.object(metadata_object_name('/' + object_to_check)).last_modified
          debugger
        rescue Aws::S3::Errors::NotFound
          # No remote metadata, local (if any) is most up to date.
          logger.info "No metadata exists on S3, skipping retrieval."
          return false
        end
        local_file = @file_store.local_path object_to_check
        return true unless File.exist? local_file
        if File.mtime(local_file) < remote_last_modified
          true
        else
          false
        end
      end

      def update_local_metadata
        @local_metadata_lock.synchronize do
          if check_if_remote_metadata_changed
            logger.info "Local metadata is out of date, repopulating from S3."
            if ! Dir.exist? Geminabox.data
              logger.info "Local data directory does not exist, creating it."
              file_store_obj = @file_store.new(nil, nil)
              file_store_obj.prepare_data_folders
            end
            (GZIPPED_FILES + TEXT_FILES).each do |metadata_filename|
              update_local_metadata_file(metadata_filename)
            end
          end
        end
      end

      # Note: deleting doesn't make much sense in this case anyway, as
      # other instances  will continue  serving cached copies  of this
      # gem (there's no way to notify them that gem has been deleted)
      #
      # Do consider using Geminabox.allow_delete = false
      #
      def delete(path_info)
        @file_store.delete path_info

        @bucket.object(gem_object_name(path_info)).delete
      end

      def update_local_file(path_info)
        gem_file = @file_store.local_path path_info

        unless File.exists? gem_file
          gem_object = @bucket.object(gem_object_name(path_info))
          if gem_object.exists?
            # Note: this will load the entire contents of the gem into
            # memory  We  might  switch   to  using  streaming  IO  or
            # temporary files  if this  proves to  be critical  in our
            # environment
            io = StringIO.new gem_object.read
            incoming_gem = Geminabox::IncomingGem.new io
            @file_store.create incoming_gem

            update_metadata
          end
        end

        @file_store.update_local_file path_info
      end

      def update_local_metadata_file(path_info)
        file_name = File.basename path_info
        pull_file file_name
      end

      def reindex &block
        FileUtils.mkpath @file_store.local_path "gems"
        @bucket.objects(prefix: "gems/").each do |object|
          path_info = "/" + object.key
          local_file_path = @file_store.local_path path_info

          file_does_not_exist = !File.exists?(local_file_path)

          # File.size raises an exception if file does not exist
          file_size = file_does_not_exist ? 0 : File.size(local_file_path)
          file_has_different_size = object.content_length != file_size

          if file_does_not_exist || file_has_different_size
            logger.info "Gem: S3 -> local #{local_file_path}"
            File.write local_file_path, object.read
          end
        end

        @file_store.reindex(&block)
      end

      private

      def update_metadata
        push_files GZIPPED_FILES

        push_files TEXT_FILES
      end

      def push_files file_list
        file_list.each do |file_name|
          push_file file_name
        end
      end

      def s3_object file_name
        object_name = metadata_object_name('/' + file_name)
        @bucket.object(object_name)
      end

      def push_file file_name
        logger.info "Push: local -> S3 #{file_name}"

        file_path = @file_store.local_path file_name
        if File.exist? file_path
          s3_object(file_name).write IO.read(file_path)
        else
          logger.info "File '#{file_path}' does not exist. Pushing an empty string to S3 instead."
          s3_object(file_name).write ""
        end
      end

      def pull_file file_name
        logger.info "Pull: S3 -> local #{file_name}"

        file_path = @file_store.local_path file_name
        File.open(file_path, 'wb') do |file|
          s3_object(file_name).read do |chunk|
            file.write(chunk)
          end
        end
      end

      def gem_object_name(path_info)
        # Remove loading slash from the path
        path_info[1..-1]
      end

      def metadata_object_name(path_info)
        path_info.gsub %r{/}, 'metadata/'
      end
    end
  end
end
