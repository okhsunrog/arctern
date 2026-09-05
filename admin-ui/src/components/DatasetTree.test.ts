// @vitest-environment happy-dom
import { expect, it } from 'vite-plus/test'
import { shallowMount } from '@vue/test-utils'
import DatasetTree from './DatasetTree.vue'

it('keeps a deep-linked dataset selected before and after datasets load', async () => {
  const wrapper = shallowMount(DatasetTree, {
    props: { modelValue: 'tank/data', datasets: [], loading: false },
  })
  const tree = wrapper.findComponent({ name: 'Tree' })
  expect(tree.props('modelValue')).toMatchObject({ value: 'tank/data' })
  await wrapper.setProps({
    datasets: [
      { name: 'tank', dataset_type: 'filesystem', properties: {} },
      { name: 'tank/data', dataset_type: 'filesystem', properties: { used: '1024' } },
    ],
  })
  expect(tree.props('modelValue')).toMatchObject({ value: 'tank/data', used: 1024 })
  tree.vm.$emit('update:modelValue', { value: 'tank', label: 'tank', used: null })
  expect(wrapper.emitted('update:modelValue')?.[0]).toEqual(['tank'])
  await wrapper.setProps({ modelValue: '' })
  expect(tree.props('modelValue')).toBeUndefined()
  wrapper.unmount()
})
