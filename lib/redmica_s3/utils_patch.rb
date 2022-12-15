module RedmicaS3
  module UtilsPatch
    extend ActiveSupport::Concern

    included do
      prepend PrependMethods
    end

    class_methods do
    end

    module PrependMethods
      def self.prepended(base)
        class << base
          self.prepend(ClassMethods)
        end
      end

      module ClassMethods
        def save_upload(upload, path)
          default_external, default_internal = Encoding.default_external, Encoding.default_internal
          Encoding.default_external = Encoding::ASCII_8BIT
          Encoding.default_internal = Encoding::ASCII_8BIT
          RedmicaS3::Connection.put_stream(path, "", upload)
          yield RedmicaS3::Connection.object_data(path).read if block_given?
        ensure
          Encoding.default_external = default_external
          Encoding.default_internal = default_internal
        end
      end
    end
  end
end