require "google/cloud/storage"
require 'aws-sdk-s3'

Aws.config[:ssl_verify_peer] = false

module RedmicaS3
  module Connection
    @@conn = nil
    @@s3_options = {
      access_key_id:      nil,
      secret_access_key:  nil,
      bucket:             nil,
      folder:             '',
      endpoint:           nil,
      thumb_folder:       'tmp',
      import_folder:      'tmp',
      region:             nil,
      service_type:       's3',
    }

    class << self
      def create_bucket
        gbucket = own_bucket
        if service_type.present? and service_type == "gcs"
          conn.create_bucket(bucket) unless gbucket.exists?
        else
          gbucket.create unless gbucket.exists?
        end
      end

      def folder
        str = @@s3_options[:folder]
        (
          if str.present?
            /\S+\/\z/.match?(str) ? str : "#{str}/"
          else
            ''
          end
        ).presence
      end

      def thumb_folder
        str = @@s3_options[:thumb_folder]
        (
          if str.present?
            /\S+\/\z/.match?(str) ? str : "#{str}/"
          else
            'tmp/'
          end
        ).presence
      end

      def import_folder
        str = @@s3_options[:import_folder]
        (
          if str.present?
            /\S+\/\z/.match?(str) ? str : "#{str}/"
          else
            'tmp/'
          end
        ).presence
      end

      def put(disk_filename, original_filename, data, content_type = 'application/octet-stream', opt = {})
        target_folder = opt[:target_folder] || self.folder
        digest = opt[:digest].presence
        options = {
          body:                 data,
          content_disposition:  "inline; filename=#{ERB::Util.url_encode(original_filename)}",
          metadata:             {},
        }
        options[:content_type] = content_type if content_type
        if digest
          options[:metadata] = {
            'digest' => digest,
          }
        end

        if service_type.present? and service_type == "gcs"
          object_nm = File.join([target_folder.presence, disk_filename.presence].compact)
          io = StringIO.new data

          own_bucket.create_file(io, object_nm,
            content_type:         options[:content_type],
            content_disposition:  options[:content_disposition],
            metadata:             options[:metadata],
          )
        else
          object = object(disk_filename, target_folder)
          object.put(options)
        end
      end

      def put_stream(disk_filename, original_filename, stream, content_type = 'application/octet-stream', opt = {})
        target_folder = opt[:target_folder] || self.folder
        options = {
          metadata:             {},
        }
        options[:content_type] = content_type if content_type

        object_nm = File.join([target_folder.presence, disk_filename.presence].compact)

        if stream.respond_to?(:read)
          if service_type.present? and service_type == "gcs"
            

            own_bucket.create_file(stream, object_nm,
              content_type:         options[:content_type],
              metadata:             options[:metadata],
            )
          else
            object = object(disk_filename, target_folder)
            object.upload_stream do |write_stream|
              buffer = ""
              while (buffer = upload.read(8192))
                write_stream << buffer.b
              end
            end
          end
        else
          self.put(disk_filename, original_filename, stream, content_type, options)
        end
      end

      def delete(filename, target_folder = self.folder)
        object = object(filename, target_folder)
        object.delete
      end

      def batch_delete(prefix)
        if service_type.present? and service_type == "gcs"
          files = own_bucket.files(prefix)
          files.each do |object|
            object.delete
          end
        else
          own_bucket.objects({prefix: prefix}).batch_delete!
        end
      end

      def object(filename, target_folder = self.folder)
        object_nm = File.join([target_folder.presence, filename.presence].compact)
        if service_type.present? and service_type == "gcs"
          return own_bucket.file(object_nm,
            skip_lookup: true,
          )
        else
          return own_bucket.object(object_nm)
        end
      end

      def object_reload(filename, reload = false, target_folder = self.folder)
        object = object(filename)

        if service_type.present? and service_type == "gcs"
          object.reload! if reload
        else
          object.reload if reload && !object.data_loaded?
        end

        return object
      end

      def object_data(filename, target_folder = self.folder)
        object = object_reload(filename, target_folder)

        if service_type.present? and service_type == "gcs"
          data = object.download
          data.rewind
          return data
        else
          return object.get.body
        end
      end

      def move_object(src_filename, dest_filename, target_folder = self.folder)
        src_object = object(src_filename, target_folder)
        return false  unless src_object.exists?
        dest_object = object(dest_filename, target_folder)
        return false  if dest_object.exists?

        if service_type.present? and service_type == "gcs"
          object_nm = File.join([target_folder.presence, dest_filename.presence].compact)

          src_object.rewrite(object_nm)
        else
          src_object.move_to(dest_object)
        end

        true
      end

      def update_object_metadata(filename, metadata = {}, target_folder = self.folder)
        object = object(src_filename, target_folder)
        return false  unless src_object.exists?

        if service_type.present? and service_type == "gcs"
          object_nm = File.join([target_folder.presence, dest_filename.presence].compact)

          object.metadata(
            object.metadata.merge(metadata)
          )
        else
          object.copy_from(object,
            content_disposition:  object.content_disposition,
            content_type:         object.content_type,
            metadata:             object.metadata.merge(metadata),
            metadata_directive:   'REPLACE'
          )
        end

        true
      end

# private

      def establish_connection
        load_options unless @@s3_options[:access_key_id] && @@s3_options[:secret_access_key]
        options = {
          access_key_id:      @@s3_options[:access_key_id],
          secret_access_key:  @@s3_options[:secret_access_key]
        }
        if endpoint.present?
          options[:endpoint] = endpoint
        elsif region.present?
          options[:region] = region
        end

        if service_type.present? and service_type == "gcs"
          @@conn = Google::Cloud::Storage.new
        else
          @@conn = Aws::S3::Resource.new(options)
        end
      end

      def load_options
        file = ERB.new( File.read(File.join(Rails.root, 'config', 's3.yml')) ).result
        # YAML.load works as YAML.safe_load if Psych >= 4.0 is installed
        (
          YAML.respond_to?(:unsafe_load) ? YAML.unsafe_load(file) : YAML.load(file)
        )[Rails.env].each do |key, value|
          @@s3_options[key.to_sym] = value
        end
      end

      def conn
        @@conn || establish_connection
      end

      def own_bucket
        conn.bucket(bucket)
      end

      def bucket
        load_options unless @@s3_options[:bucket]
        @@s3_options[:bucket]
      end

      def endpoint
        @@s3_options[:endpoint]
      end

      def region
        @@s3_options[:region]
      end

      def service_type
        @@s3_options[:service_type]
      end
    end

    private_class_method  :establish_connection, :load_options, :conn, :own_bucket, :bucket, :endpoint, :region, :service_type
  end
end
